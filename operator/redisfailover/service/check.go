package service

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	redisauth "github.com/spotahome/redis-operator/operator/redisfailover/auth"
	"github.com/spotahome/redis-operator/service/k8s"
	"github.com/spotahome/redis-operator/service/redis"
)

// RedisFailoverCheck defines the interface able to check the correct status of a redis failover
type RedisFailoverCheck interface {
	CheckRedisNumber(rFailover *redisfailoverv1.RedisFailover) error
	CheckSentinelNumber(rFailover *redisfailoverv1.RedisFailover) error
	CheckAllSlavesFromMaster(master string, rFailover *redisfailoverv1.RedisFailover) error
	CheckSentinelNumberInMemory(sentinel string, rFailover *redisfailoverv1.RedisFailover) error
	CheckSentinelSlavesNumberInMemory(sentinel string, rFailover *redisfailoverv1.RedisFailover) error
	CheckSentinelMonitor(sentinel string, monitor ...string) error
	GetMasterIP(rFailover *redisfailoverv1.RedisFailover) (string, error)
	GetNumberMasters(rFailover *redisfailoverv1.RedisFailover) (int, error)
	GetRedisesIPs(rFailover *redisfailoverv1.RedisFailover) ([]string, error)
	GetSentinelsIPs(rFailover *redisfailoverv1.RedisFailover) ([]string, error)
	GetMinimumRedisPodTime(rFailover *redisfailoverv1.RedisFailover) (time.Duration, error)
	GetRedisesSlavesPods(rFailover *redisfailoverv1.RedisFailover) ([]string, error)
	GetRedisesMasterPod(rFailover *redisfailoverv1.RedisFailover) (string, error)
	GetStatefulSetUpdateRevision(rFailover *redisfailoverv1.RedisFailover) (string, error)
	GetRedisRevisionHash(podName string, rFailover *redisfailoverv1.RedisFailover) (string, error)
	CheckRedisSlavesReady(slaveIP string, rFailover *redisfailoverv1.RedisFailover) (bool, error)
	GetDesiredUsers(rFailover *redisfailoverv1.RedisFailover) (map[string]redisfailoverv1.UserSpec, error)
	GetRedisUsersAsString(rFailover *redisfailoverv1.RedisFailover) ([]string, error)
	ShouldProcessRedisUsers(rFailover *redisfailoverv1.RedisFailover) bool
}

// RedisFailoverChecker is our implementation of RedisFailoverCheck interface
type RedisFailoverChecker struct {
	k8sService  k8s.Services
	redisClient redis.Client
	logger      log.Logger
}

// NewRedisFailoverChecker creates an object of the RedisFailoverChecker struct
func NewRedisFailoverChecker(k8sService k8s.Services, redisClient redis.Client, logger log.Logger) *RedisFailoverChecker {
	return &RedisFailoverChecker{
		k8sService:  k8sService,
		redisClient: redisClient,
		logger:      logger,
	}
}

// CheckRedisNumber controlls that the number of deployed redis is the same than the requested on the spec
func (r *RedisFailoverChecker) CheckRedisNumber(rf *redisfailoverv1.RedisFailover) error {
	ss, err := r.k8sService.GetStatefulSet(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}
	if rf.Spec.Redis.Replicas != *ss.Spec.Replicas {
		return errors.New("number of redis pods differ from specification")
	}
	return nil
}

// CheckSentinelNumber controlls that the number of deployed sentinel is the same than the requested on the spec
func (r *RedisFailoverChecker) CheckSentinelNumber(rf *redisfailoverv1.RedisFailover) error {
	d, err := r.k8sService.GetDeployment(rf.Namespace, GetSentinelName(rf))
	if err != nil {
		return err
	}
	if rf.Spec.Sentinel.Replicas != *d.Spec.Replicas {
		return errors.New("number of sentinel pods differ from specification")
	}
	return nil
}

func (r *RedisFailoverChecker) setMasterLabelIfNecessary(namespace string, pod corev1.Pod) error {
	for labelKey, labelValue := range pod.ObjectMeta.Labels {
		if labelKey == redisRoleLabelKey && labelValue == redisRoleLabelMaster {
			return nil
		}
	}
	return r.k8sService.UpdatePodLabels(namespace, pod.ObjectMeta.Name, generateRedisMasterRoleLabel())
}

func (r *RedisFailoverChecker) setSlaveLabelIfNecessary(namespace string, pod corev1.Pod) error {
	for labelKey, labelValue := range pod.ObjectMeta.Labels {
		if labelKey == redisRoleLabelKey && labelValue == redisRoleLabelSlave {
			return nil
		}
	}
	return r.k8sService.UpdatePodLabels(namespace, pod.ObjectMeta.Name, generateRedisSlaveRoleLabel())
}

// CheckAllSlavesFromMaster controlls that all slaves have the same master (the real one)
func (r *RedisFailoverChecker) CheckAllSlavesFromMaster(master string, rf *redisfailoverv1.RedisFailover) error {
	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}

	authProvider := redisauth.GetAuthProvider(rf, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return err
	}

	rport := getRedisPort(rf.Spec.Redis.Port)
	for _, rp := range rps.Items {
		if rp.Status.PodIP == master {
			err = r.setMasterLabelIfNecessary(rf.Namespace, rp)
			if err != nil {
				return err
			}
		} else {
			err = r.setSlaveLabelIfNecessary(rf.Namespace, rp)
			if err != nil {
				return err
			}
		}

		slave, err := r.redisClient.GetSlaveOf(rp.Status.PodIP, rport, username, password)
		if err != nil {
			r.logger.Errorf("Get slave of master failed, maybe this node is not ready, pod ip: %s", rp.Status.PodIP)
			return err
		}
		if slave != "" && slave != master {
			return fmt.Errorf("slave %s don't have the master %s, has %s", rp.Status.PodIP, master, slave)
		}
	}
	return nil
}

// CheckSentinelNumberInMemory controls that the provided sentinel has only the living sentinels on its memory.
func (r *RedisFailoverChecker) CheckSentinelNumberInMemory(sentinel string, rf *redisfailoverv1.RedisFailover) error {
	nSentinels, err := r.redisClient.GetNumberSentinelsInMemory(sentinel)
	if err != nil {
		return err
	} else if nSentinels != rf.Spec.Sentinel.Replicas {
		return errors.New("sentinels in memory mismatch")
	}
	return nil
}

// CheckSentinelSlavesNumberInMemory controls that the provided sentinel has only the expected slaves number.
func (r *RedisFailoverChecker) CheckSentinelSlavesNumberInMemory(sentinel string, rf *redisfailoverv1.RedisFailover) error {
	nSlaves, err := r.redisClient.GetNumberSentinelSlavesInMemory(sentinel)
	if err != nil {
		return err
	} else if nSlaves != rf.Spec.Redis.Replicas-1 {
		return errors.New("redis slaves in sentinel memory mismatch")
	}
	return nil
}

// CheckSentinelMonitor controls if the sentinels are monitoring the expected master
func (r *RedisFailoverChecker) CheckSentinelMonitor(sentinel string, monitor ...string) error {
	monitorIP := monitor[0]
	monitorPort := ""
	if len(monitor) > 1 {
		monitorPort = monitor[1]
	}
	actualMonitorIP, actualMonitorPort, err := r.redisClient.GetSentinelMonitor(sentinel)
	if err != nil {
		return err
	}
	if actualMonitorIP != monitorIP || (monitorPort != "" && monitorPort != actualMonitorPort) {
		return errors.New("the monitor on the sentinel config does not match with the expected one")
	}
	return nil
}

// GetMasterIP connects to all redis and returns the master of the redis failover
func (r *RedisFailoverChecker) GetMasterIP(rf *redisfailoverv1.RedisFailover) (string, error) {
	rips, err := r.GetRedisesIPs(rf)
	if err != nil {
		return "", err
	}

	authProvider := redisauth.GetAuthProvider(rf, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return "", err
	}

	masters := []string{}
	rport := getRedisPort(rf.Spec.Redis.Port)
	for _, rip := range rips {
		master, err := r.redisClient.IsMaster(rip, rport, username, password)
		if err != nil {
			r.logger.Errorf("Get redis info failed, maybe this node is not ready, pod ip: %s ; error is: %s", rip, err.Error())
			continue
		}
		if master {
			masters = append(masters, rip)
		}
	}

	if len(masters) != 1 {
		return "", errors.New("number of redis nodes known as master is different than 1")
	}
	return masters[0], nil
}

// GetNumberMasters returns the number of redis nodes that are working as a master
func (r *RedisFailoverChecker) GetNumberMasters(rf *redisfailoverv1.RedisFailover) (int, error) {
	nMasters := 0
	rips, err := r.GetRedisesIPs(rf)
	if err != nil {
		return nMasters, err
	}

	authProvider := redisauth.GetAuthProvider(rf, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return nMasters, err
	}

	rport := getRedisPort(rf.Spec.Redis.Port)
	for _, rip := range rips {
		master, err := r.redisClient.IsMaster(rip, rport, username, password)
		if err != nil {
			r.logger.Errorf("Get redis info failed, maybe this node is not ready, pod ip: %s; error is: %v", rip, err.Error)
			continue
		}
		if master {
			nMasters++
		}
	}
	return nMasters, nil
}

// GetRedisesIPs returns the IPs of the Redis nodes
func (r *RedisFailoverChecker) GetRedisesIPs(rf *redisfailoverv1.RedisFailover) ([]string, error) {
	redises := []string{}
	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return nil, err
	}
	for _, rp := range rps.Items {
		if rp.Status.Phase == corev1.PodRunning && rp.DeletionTimestamp == nil { // Only work with running pods
			redises = append(redises, rp.Status.PodIP)
		}
	}
	return redises, nil
}

// GetSentinelsIPs returns the IPs of the Sentinel nodes
func (r *RedisFailoverChecker) GetSentinelsIPs(rf *redisfailoverv1.RedisFailover) ([]string, error) {
	sentinels := []string{}
	rps, err := r.k8sService.GetDeploymentPods(rf.Namespace, GetSentinelName(rf))
	if err != nil {
		return nil, err
	}
	for _, sp := range rps.Items {
		if sp.Status.Phase == corev1.PodRunning && sp.DeletionTimestamp == nil { // Only work with running pods
			sentinels = append(sentinels, sp.Status.PodIP)
		}
	}
	return sentinels, nil
}

// GetMinimumRedisPodTime returns the minimum time a pod is alive
func (r *RedisFailoverChecker) GetMinimumRedisPodTime(rf *redisfailoverv1.RedisFailover) (time.Duration, error) {
	minTime := 100000 * time.Hour // More than ten years
	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return minTime, err
	}
	for _, redisNode := range rps.Items {
		if redisNode.Status.StartTime == nil {
			continue
		}
		start := redisNode.Status.StartTime.Round(time.Second)
		alive := time.Since(start)
		r.logger.Debugf("Pod %s has been alive for %.f seconds", redisNode.Status.PodIP, alive.Seconds())
		if alive < minTime {
			minTime = alive
		}
	}
	return minTime, nil
}

// GetRedisesSlavesPods returns pods names of the Redis slave nodes
func (r *RedisFailoverChecker) GetRedisesSlavesPods(rf *redisfailoverv1.RedisFailover) ([]string, error) {
	redises := []string{}
	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return nil, err
	}

	authProvider := redisauth.GetAuthProvider(rf, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return redises, err
	}

	rport := getRedisPort(rf.Spec.Redis.Port)
	for _, rp := range rps.Items {
		if rp.Status.Phase == corev1.PodRunning && rp.DeletionTimestamp == nil { // Only work with running
			master, err := r.redisClient.IsMaster(rp.Status.PodIP, rport, username, password)
			if err != nil {
				return []string{}, err
			}
			if !master {
				redises = append(redises, rp.ObjectMeta.Name)
			}
		}
	}
	return redises, nil
}

// GetRedisesMasterPod returns pods names of the Redis slave nodes
func (r *RedisFailoverChecker) GetRedisesMasterPod(rFailover *redisfailoverv1.RedisFailover) (string, error) {
	rps, err := r.k8sService.GetStatefulSetPods(rFailover.Namespace, GetRedisName(rFailover))
	if err != nil {
		return "", err
	}

	authProvider := redisauth.GetAuthProvider(rFailover, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return "", err
	}

	rport := getRedisPort(rFailover.Spec.Redis.Port)
	for _, rp := range rps.Items {
		if rp.Status.Phase == corev1.PodRunning && rp.DeletionTimestamp == nil { // Only work with running
			master, err := r.redisClient.IsMaster(rp.Status.PodIP, rport, username, password)
			if err != nil {
				return "", err
			}
			if master {
				return rp.ObjectMeta.Name, nil
			}
		}
	}
	return "", errors.New("redis nodes known as master not found")
}

// GetStatefulSetUpdateRevision returns current version for the statefulSet
// If the label don't exists, we return an empty value and no error, so previous versions don't break
func (r *RedisFailoverChecker) GetStatefulSetUpdateRevision(rFailover *redisfailoverv1.RedisFailover) (string, error) {
	ss, err := r.k8sService.GetStatefulSet(rFailover.Namespace, GetRedisName(rFailover))
	if err != nil {
		return "", err
	}

	if ss == nil {
		return "", errors.New("statefulSet not found")
	}

	return ss.Status.UpdateRevision, nil
}

// GetRedisRevisionHash returns the statefulset uid for the pod
func (r *RedisFailoverChecker) GetRedisRevisionHash(podName string, rFailover *redisfailoverv1.RedisFailover) (string, error) {
	pod, err := r.k8sService.GetPod(rFailover.Namespace, podName)
	if err != nil {
		return "", err
	}

	if pod == nil {
		return "", errors.New("pod not found")
	}

	if pod.ObjectMeta.Labels == nil {
		return "", errors.New("labels not found")
	}

	val := pod.ObjectMeta.Labels[appsv1.ControllerRevisionHashLabelKey]

	return val, nil
}

// CheckRedisSlavesReady returns true if the slave is ready (sync, connected, etc)
func (r *RedisFailoverChecker) CheckRedisSlavesReady(ip string, rFailover *redisfailoverv1.RedisFailover) (bool, error) {
	authProvider := redisauth.GetAuthProvider(rFailover, r.k8sService)
	username, password, err := authProvider.GetAdminCredentials()
	if err != nil {
		return false, err
	}
	port := getRedisPort(rFailover.Spec.Redis.Port)
	return r.redisClient.SlaveIsReady(ip, port, username, password)
}

func getRedisPort(p int32) string {
	return strconv.Itoa(int(p))
}

/*
	Returns users that are required to be present in redis as specified in CR

inputs:

	*redisfailoverv1.RedisFailover (CR object ptr)

outputs:

	[map]redisfailoverv1.UserSpec (a map of username -> userSpec )
*/
func (r *RedisFailoverChecker) GetDesiredUsers(rFailover *redisfailoverv1.RedisFailover) (map[string]redisfailoverv1.UserSpec, error) {
	authProvider := redisauth.GetAuthProvider(rFailover, r.k8sService)
	return authProvider.InterceptUsers(rFailover.Spec.AuthV2.Users, rFailover.Namespace, r.k8sService)

}

/*
Returns redis users as seen in redis (master instance). format will be same as seen in `acl list`
example output:
```
[

	"user default on #37a8eec1ce19687d132fe29051dca629d164e2c4958ba141d5f4133a33f0688f ~* &* +@all",
	"user admin on #bbd7182cd0ee95488f1a1e6f3fe0d8f94ed0d14e4db1dce713fe82a3231c523d ~* &* -@all +@admin +xlen +slowlog +llen +memory +cluster|info +get +pfcount +type +xinfo +hlen +zcard +client +eval +latency +info +strlen +ping +scan +select +scard""

]
```

inputs:

	*redisfailoverv1.RedisFailover (CR object ptr)

output:

	[]string (`acl list` output as string)
*/
func (r *RedisFailoverChecker) GetRedisUsersAsString(rFailover *redisfailoverv1.RedisFailover) ([]string, error) {
	authProvider := redisauth.GetAuthProvider(rFailover, r.k8sService)
	adminUser, adminPassword, err := authProvider.GetAdminCredentials()
	if nil != err {
		return nil, err
	}
	masterIP, err := r.GetMasterIP(rFailover)
	if err != nil {
		log.WithField("namespace", rFailover.Namespace).WithField("resource", rFailover.Name).Errorf("unable to get redis users because master IP cannot be resolved.")
		return nil, err
	}
	port := getRedisPort(rFailover.Spec.Redis.Port)
	if err != nil {
		return nil, err
	}
	return r.redisClient.GetUsers(masterIP, port, adminUser, adminPassword)
}

/*
	Should we run `check and heal` loop on redis users? No if legacy auth model is followed. Yes if `authV2` or something newer is used

inputs:

	*redisfailoverv1.RedisFailover (CR object ptr)

output:

	bool ( `true` if we should run check and heal reconcile loop )
*/
func (r *RedisFailoverChecker) ShouldProcessRedisUsers(rFailover *redisfailoverv1.RedisFailover) bool {
	authProvider := redisauth.GetAuthProvider(rFailover, r.k8sService)
	log.Debugf("authprovider for resource: %v is %v", rFailover.Name, authProvider.Version())
	if "V1" == authProvider.Version() {
		return false
	}
	return true
}
