package scalers

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2beta2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultTargetPendingGitLabJobsQueueLength = 1
)

type gitLabRunnerScaler struct {
	metadata   *gitLabRunnerMetadata
	httpClient *http.Client
}

type gitLabRunnerMetadata struct {
	instanceURL string
	token       string
	groups      []string
	projects    []string
	runnerTags  []string
	targetJobs  int
	scalerIndex int
}

var gitLabRunnerLog = logf.Log.WithName("gitlab_runner_scaler")

// NewGitLabRunnerScaler creates a new GitLabRunnerScaler
func NewGitLabRunnerScaler(config *ScalerConfig) (Scaler, error) {
	meta, err := parseGitLabRunnerMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing GitLab Runner metadata: %s", err)
	}

	httpClient := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false)

	return &gitLabRunnerScaler{
		metadata:   meta,
		httpClient: httpClient,
	}, nil
}

func parseGitLabRunnerMetadata(config *ScalerConfig) (*gitLabRunnerMetadata, error) {
	meta := gitLabRunnerMetadata{}
	meta.targetJobs = defaultTargetPendingGitLabJobsQueueLength

	if val, ok := config.TriggerMetadata["targetPendingGitLabJobsQueueLength"]; ok {
		queueLength, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing gitlab runner metadata targetPendingGitLabJobsQueueLength: %s", err.Error())
		}

		meta.targetJobs = queueLength
	}

	if val, ok := config.AuthParams["instanceURL"]; ok && val != "" {
		// Found the organizationURL in a parameter from TriggerAuthentication
		meta.instanceURL = val
	} else if val, ok := config.TriggerMetadata["instanceURLFromEnv"]; ok && val != "" {
		meta.instanceURL = config.ResolvedEnv[val]
	} else {
		return nil, fmt.Errorf("no instanceURL given")
	}

	if val, ok := config.AuthParams["token"]; ok && val != "" {
		// Found the token in a parameter from TriggerAuthentication
		meta.token = config.AuthParams["token"]
	} else if val, ok := config.TriggerMetadata["tokenFromEnv"]; ok && val != "" {
		meta.token = config.ResolvedEnv[config.TriggerMetadata["tokenFromEnv"]]
	} else {
		return nil, fmt.Errorf("no token given")
	}

	meta.scalerIndex = config.ScalerIndex

	return &meta, nil
}

func (s *gitLabRunnerScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	queuelen, err := s.GetGitLabRunnerJobsQueueLength(ctx)

	if err != nil {
		azurePipelinesLog.Error(err, "error getting pipelines queue length")
		return []external_metrics.ExternalMetricValue{}, err
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(int64(queuelen), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *gitLabRunnerScaler) GetGitLabRunnerJobsQueueLength(ctx context.Context) (int, error) {
	url := fmt.Sprintf("%s/_apis/distributedtask/pools/jobrequests", s.metadata.instanceURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return -1, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", s.metadata.token))

	r, err := s.httpClient.Do(req)
	if err != nil {
		return -1, err
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return -1, err
	}
	r.Body.Close()

	if !(r.StatusCode >= 200 && r.StatusCode <= 299) {
		return -1, fmt.Errorf("the GitLab API returned error. url: %s status: %d response: %s", url, r.StatusCode, string(b))
	}

	var result map[string]interface{}
	err = json.Unmarshal(b, &result)
	if err != nil {
		return -1, err
	}

	var count = 0
	jobs, ok := result["value"].([]interface{})

	if !ok {
		return -1, fmt.Errorf("the GitLab API result returned no value data. url: %s status: %d", url, r.StatusCode)
	}

	for _, value := range jobs {
		v := value.(map[string]interface{})
		if v["result"] == nil {
			count++
		}
	}

	return count, err
}

func (s *gitLabRunnerScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	targetJobsQueueLengthQty := resource.NewQuantity(int64(s.metadata.targetJobs), resource.DecimalSI)
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, kedautil.NormalizeString(fmt.Sprintf("azure-pipelines-%s", s.metadata.poolID))),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetJobsQueueLengthQty,
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

func (s *gitLabRunnerScaler) IsActive(ctx context.Context) (bool, error) {
	queuelen, err := s.GetGitLabRunnerJobsQueueLength(ctx)

	if err != nil {
		gitLabRunnerLog.Error(err, "error)")
		return false, err
	}

	return queuelen > 0, nil
}

func (s *gitLabRunnerScaler) Close(context.Context) error {
	return nil
}
