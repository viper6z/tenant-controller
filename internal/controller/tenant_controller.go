/*
Copyright 2026.

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

	platformv1alpha1 "github.com/viper6z/tenant-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const finalizerName = "platform.viper6z.dev/finalizer"

// TenantReconciler reconciles a Tenant object
type TenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=platform.viper6z.dev,resources=tenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.viper6z.dev,resources=tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.viper6z.dev,resources=tenants/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Tenant object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// TODO(user): your logic here
	var tenant platformv1alpha1.Tenant
	if err := r.Get(ctx, req.NamespacedName, &tenant); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !tenant.DeletionTimestamp.IsZero() {
		ns := corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: tenant.Name},
		}
		if err := r.Delete(ctx, &ns); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(&tenant, finalizerName)
		if err := r.Update(ctx, &tenant); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&tenant, finalizerName) {
		controllerutil.AddFinalizer(&tenant, finalizerName)
		if err := r.Update(ctx, &tenant); err != nil {
			return ctrl.Result{}, err
		}
	}

	var namespace corev1.Namespace

	if err := r.Get(ctx, client.ObjectKey{Name: tenant.Name}, &namespace); err != nil {
		if apierrors.IsNotFound(err) {
			var ns corev1.Namespace
			ns.Name = tenant.Name
			if err := r.Create(ctx, &ns); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	quota := corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-quota",
			Namespace: tenant.Name,
		},
	}
	cpuQuantity, err := resource.ParseQuantity(tenant.Spec.Quota.CPU)
	if err != nil {
		return ctrl.Result{}, err
	}
	memQuantity, err := resource.ParseQuantity(tenant.Spec.Quota.Memory)
	if err != nil {
		return ctrl.Result{}, err
	}
	podQuantity, err := resource.ParseQuantity(tenant.Spec.Quota.Pods)
	if err != nil {
		return ctrl.Result{}, err
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, &quota, func() error {
		quota.Spec.Hard = corev1.ResourceList{
			corev1.ResourceLimitsCPU:    cpuQuantity,
			corev1.ResourceLimitsMemory: memQuantity,
			corev1.ResourcePods:         podQuantity,
		}
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	log := logf.FromContext(ctx)
	log.Info("reconciled quota", "operation", result)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Tenant{}).
		Named("tenant").
		Complete(r)
}
