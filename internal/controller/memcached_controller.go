/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"os"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/example/memcached-operator/api/v1alpha1"
	"github.com/pkg/errors"
)

const finalizerName = "cache.example.com/finalizer"

// Definitions to manage status conditions
const (
	// typeAvailableMemcached represents the status of the Deployment reconciliation
	typeAvailableMemcached = "Available"
	// typeDegradedMemcached represents the status used when the custom resource is deleted and the finalizer operations are must to occur.
	typeDegradedMemcached = "Degraded"
)

// MemcachedReconciler reconciles a Memcached object
type MemcachedReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Memcached object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
// +kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.vahidmostofi.com,resources=memcacheds/finalizers,verbs=update
func (r *MemcachedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	memcached := &cachev1alpha1.Memcached{}

	err := r.Get(ctx, req.NamespacedName, memcached)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Memcached resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}

		log.Error(err, "Failed to get Memcached")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Memcached")
	}

	// if the status is empty, update it to unknown
	if memcached.Status.Conditions == nil || len(memcached.Status.Conditions) == 0 {
		unknownStatus := metav1.Condition{
			Type:    typeAvailableMemcached,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: "Starting reconciliation",
		}
		meta.SetStatusCondition(&memcached.Status.Conditions, unknownStatus)
		if err := r.Status().Update(ctx, memcached); err != nil {
			log.Error(err, "Failed to update Memcached status")
			return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached status")
		}

		if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
			log.Error(err, "Failed to get Memcached")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Memcached after update")
		}
	}

	if !controllerutil.ContainsFinalizer(memcached, finalizerName) {
		log.Info("Adding finalizer to Memcached")

		if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
			log.Error(err, "Failed to get Memcached")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Memcached")
		}

		if ok := controllerutil.AddFinalizer(memcached, finalizerName); !ok {
			log.Error(err, "Failed to add finalizer to Memcached")
			return ctrl.Result{Requeue: true}, nil
		}

		if err := r.Update(ctx, memcached); err != nil {
			log.Error(err, "Failed to update Memcached with finalizer")
			return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached with finalizer")
		}
	}

	// Check if the Memcached instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isMemcachedMarkedToBeDeleted := memcached.GetDeletionTimestamp() != nil
	if isMemcachedMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(memcached, finalizerName) {
			log.Info("Performing finalizer operation for Memcached before deleting")

			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{
				Type:    typeDegradedMemcached,
				Status:  metav1.ConditionUnknown,
				Reason:  "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s", memcached.Name),
			})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached status")
			}

			r.doFinalizerOperationsForMemcached(memcached)

			// TODO make sure all operations are done successfully

			if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
				log.Error(err, "Failed to get Memcached")
				return ctrl.Result{}, errors.Wrap(err, "failed to get Memcached after finalizer operations")
			}

			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{
				Type:    typeDegradedMemcached,
				Status:  metav1.ConditionTrue,
				Reason:  "Finalizing",
				Message: fmt.Sprintf("Finalizer operations for the custom resource: %s are done successfully", memcached.Name),
			})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached status")
			}

			// remove the finalizer
			if ok := controllerutil.RemoveFinalizer(memcached, finalizerName); !ok {
				log.Error(err, "Failed to remove finalizer from Memcached")
				return ctrl.Result{}, nil
			}

			if err := r.Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached")
				return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached")
			}
		}
	}

	// Check if the deployment already exists, if not create a new one
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: memcached.Name, Namespace: memcached.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// define a new deployment
		dep, err := r.deploymentForMemcached(memcached)
		if err != nil {
			log.Error(err, "Failed to create Deployment")

			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{
				Type:    typeAvailableMemcached,
				Status:  metav1.ConditionFalse,
				Reason:  "Reconciling",
				Message: fmt.Sprintf("Failed to create Deployment for custom resource (%s)", memcached.Name),
			})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, errors.Wrap(err, "Failed to update Memcached status")
			}

			return ctrl.Result{}, errors.Wrap(err, "Failed to create Deployment")
		}

		log.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			if apierrors.IsAlreadyExists(err) {
				log.Info("Deployment already exists", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
				return ctrl.Result{}, nil
			}
			log.Error(err, "failed to create Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return ctrl.Result{}, errors.Wrap(err, "Failed to create Deployment")
		}

		return ctrl.Result{}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Deployment")
		return ctrl.Result{}, errors.Wrap(err, "Failed to get Deployment")
	}

	// Insure the deployment has the desired state defined in the CRD API
	size := memcached.Spec.Size
	port := memcached.Spec.ContainerPort
	if *found.Spec.Replicas != size || found.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort != port {
		found.Spec.Replicas = &size
		found.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort = port
		// update the state
		// if failed, -> log, get the latest sate, update the state to not available, update the state, return the error
		// ow -> no we requeue the result so the reconcile will be called again
		//       we want this function runs enough time till the state is updated
		//       and *found.Spec.Replicas != size is false
		if err = r.Update(ctx, found); err != nil {
			log.Error(err, "Failed to update Deployment",
				"Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)

			// Re-fetch the memcached Custom Resource before update the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raise the issue "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
				log.Error(err, "Failed to re-fetch memcached")
				return ctrl.Result{}, err
			}

			// The following implementation will update the status
			meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{Type: typeAvailableMemcached,
				Status: metav1.ConditionFalse, Reason: "Resizing",
				Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", memcached.Name, err)})

			if err := r.Status().Update(ctx, memcached); err != nil {
				log.Error(err, "Failed to update Memcached status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.Get(ctx, req.NamespacedName, memcached); err != nil {
		log.Error(err, "Failed to re-fetch memcached")
		return ctrl.Result{}, err
	}

	// The following implementation will update the status
	meta.SetStatusCondition(&memcached.Status.Conditions, metav1.Condition{Type: typeAvailableMemcached,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Deployment for custom resource (%s) with %d replicas created successfully", memcached.Name, size)})

	if err := r.Status().Update(ctx, memcached); err != nil {
		log.Error(err, "Failed to update Memcached status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// finalizeMemcached will perform the required operations before delete the CR.
func (r *MemcachedReconciler) doFinalizerOperationsForMemcached(cr *cachev1alpha1.Memcached) {
	// TODO(user): Add the cleanup steps that the operator
	// needs to do before the CR can be deleted. Examples
	// of finalizers include performing backups and deleting
	// resources that are not owned by this CR, like a PVC.

	// Note: It is not recommended to use finalizers with the purpose of delete resources which are
	// created and managed in the reconciliation. These ones, such as the Deployment created on this reconcile,
	// are defined as depended of the custom resource. See that we use the method ctrl.SetControllerReference.
	// to set the ownerRef which means that the Deployment will be deleted by the Kubernetes API.
	// More info: https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/

	// The following implementation will raise an event
	r.Recorder.Event(cr, "Warning", "Deleting",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			cr.Name,
			cr.Namespace))
}

// deploymentForMemcached returns a Memcached Deployment object
func (r *MemcachedReconciler) deploymentForMemcached(
	memcached *cachev1alpha1.Memcached) (*appsv1.Deployment, error) {
	ls := labelsForMemcached(memcached.Name)
	replicas := memcached.Spec.Size

	// Get the Operand image
	image, err := imageForMemcached()
	if err != nil {
		return nil, err
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memcached.Name,
			Namespace: memcached.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					// TODO(user): Uncomment the following code to configure the nodeAffinity expression
					// according to the platforms which are supported by your solution. It is considered
					// best practice to support multiple architectures. build your manager image using the
					// makefile target docker-buildx. Also, you can use docker manifest inspect <image>
					// to check what are the platforms supported.
					// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
					//Affinity: &corev1.Affinity{
					//	NodeAffinity: &corev1.NodeAffinity{
					//		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					//			NodeSelectorTerms: []corev1.NodeSelectorTerm{
					//				{
					//					MatchExpressions: []corev1.NodeSelectorRequirement{
					//						{
					//							Key:      "kubernetes.io/arch",
					//							Operator: "In",
					//							Values:   []string{"amd64", "arm64", "ppc64le", "s390x"},
					//						},
					//						{
					//							Key:      "kubernetes.io/os",
					//							Operator: "In",
					//							Values:   []string{"linux"},
					//						},
					//					},
					//				},
					//			},
					//		},
					//	},
					//},
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &[]bool{true}[0],
						// IMPORTANT: seccomProfile was introduced with Kubernetes 1.19
						// If you are looking for to produce solutions to be supported
						// on lower versions you must remove this option.
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "memcached",
						ImagePullPolicy: corev1.PullIfNotPresent,
						// Ensure restrictive context for the container
						// More info: https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
						SecurityContext: &corev1.SecurityContext{
							// WARNING: Ensure that the image used defines an UserID in the Dockerfile
							// otherwise the Pod will not run and will fail with "container has runAsNonRoot and image has non-numeric user"".
							// If you want your workloads admitted in namespaces enforced with the restricted mode in OpenShift/OKD vendors
							// then, you MUST ensure that the Dockerfile defines a User ID OR you MUST leave the "RunAsNonRoot" and
							// "RunAsUser" fields empty.
							RunAsNonRoot: &[]bool{true}[0],
							// The memcached image does not use a non-zero numeric user as the default user.
							// Due to RunAsNonRoot field being set to true, we need to force the user in the
							// container to a non-zero numeric user. We do this using the RunAsUser field.
							// However, if you are looking to provide solution for K8s vendors like OpenShift
							// be aware that you cannot run under its restricted-v2 SCC if you set this value.
							RunAsUser:                &[]int64{1001}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{
									"ALL",
								},
							},
						},
						Ports: []corev1.ContainerPort{{
							ContainerPort: memcached.Spec.ContainerPort,
							Name:          "memcached",
						}},
						Command: []string{"memcached", "-m=64", "-o", "modern", "-v"},
					}},
				},
			},
		},
	}

	// Set the ownerRef for the Deployment
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
	if err := ctrl.SetControllerReference(memcached, dep, r.Scheme); err != nil {
		return nil, err
	}
	return dep, nil
}

// labelsForMemcached returns the labels for selecting the resources
// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func labelsForMemcached(name string) map[string]string {
	var imageTag string
	image, err := imageForMemcached()
	if err == nil {
		imageTag = strings.Split(image, ":")[1]
	}
	return map[string]string{"app.kubernetes.io/name": "Memcached",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/version":    imageTag,
		"app.kubernetes.io/part-of":    "memcached-operator",
		"app.kubernetes.io/created-by": "controller-manager",
	}
}

// imageForMemcached gets the Operand image which is managed by this controller
// from the MEMCACHED_IMAGE environment variable defined in the config/manager/manager.yaml
func imageForMemcached() (string, error) {
	var imageEnvVar = "MEMCACHED_IMAGE"
	image, found := os.LookupEnv(imageEnvVar)
	if !found {
		return "", fmt.Errorf("Unable to find %s environment variable with the image", imageEnvVar)
	}
	return image, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MemcachedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Memcached{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
