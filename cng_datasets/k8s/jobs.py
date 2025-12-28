"""
Kubernetes job generation and management.

Tools for creating and submitting Kubernetes jobs for large-scale
dataset processing on clusters.
"""

from typing import Optional, Dict, Any, List
import yaml
from pathlib import Path


class K8sJobManager:
    """
    Manage Kubernetes jobs for dataset processing.
    
    Generates job YAML configurations and submits them to a Kubernetes cluster.
    """
    
    def __init__(
        self,
        namespace: str = "biodiversity",
        image: str = "python:3.12-slim",
        service_account: Optional[str] = None,
        secrets: Optional[List[str]] = None,
    ):
        """
        Initialize the K8s job manager.
        
        Args:
            namespace: Kubernetes namespace for jobs
            image: Container image to use
            service_account: Service account name for jobs
            secrets: List of secret names to mount
        """
        self.namespace = namespace
        self.image = image
        self.service_account = service_account
        self.secrets = secrets or []
    
    def generate_job_yaml(
        self,
        job_name: str,
        command: List[str],
        args: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        cpu: str = "1",
        memory: str = "4Gi",
        completions: int = 1,
        parallelism: int = 1,
        restart_policy: str = "OnFailure",
    ) -> Dict[str, Any]:
        """
        Generate Kubernetes job specification.
        
        Args:
            job_name: Name for the job
            command: Container command
            args: Command arguments
            env_vars: Environment variables
            cpu: CPU request/limit
            memory: Memory request/limit
            completions: Number of job completions needed
            parallelism: Number of parallel pods
            restart_policy: Pod restart policy
            
        Returns:
            Job specification as dictionary
        """
        job_spec = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
            },
            "spec": {
                "completions": completions,
                "parallelism": parallelism,
                "template": {
                    "spec": {
                        "restartPolicy": restart_policy,
                        "containers": [{
                            "name": "processor",
                            "image": self.image,
                            "command": command,
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"cpu": cpu, "memory": memory},
                            }
                        }]
                    }
                }
            }
        }
        
        # Add arguments if provided
        if args:
            job_spec["spec"]["template"]["spec"]["containers"][0]["args"] = args
        
        # Add environment variables
        if env_vars:
            env_list = [{"name": k, "value": v} for k, v in env_vars.items()]
            job_spec["spec"]["template"]["spec"]["containers"][0]["env"] = env_list
        
        # Add service account if specified
        if self.service_account:
            job_spec["spec"]["template"]["spec"]["serviceAccountName"] = self.service_account
        
        # Add secrets if specified
        if self.secrets:
            volumes = []
            volume_mounts = []
            for secret in self.secrets:
                volumes.append({
                    "name": secret,
                    "secret": {"secretName": secret}
                })
                volume_mounts.append({
                    "name": secret,
                    "mountPath": f"/secrets/{secret}",
                    "readOnly": True
                })
            job_spec["spec"]["template"]["spec"]["volumes"] = volumes
            job_spec["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] = volume_mounts
        
        return job_spec
    
    def generate_chunked_job(
        self,
        job_name: str,
        script_path: str,
        num_chunks: int,
        base_args: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Generate a job for processing data in chunks.
        
        Args:
            job_name: Base name for the job
            script_path: Path to processing script in container
            num_chunks: Total number of chunks to process
            base_args: Base arguments for the script
            **kwargs: Additional arguments passed to generate_job_yaml
            
        Returns:
            Job specification with indexed completions
        """
        # Use indexed completions for chunk processing
        args = base_args or []
        args.extend(["--i", "$(INDEX)"])
        
        job_spec = self.generate_job_yaml(
            job_name=job_name,
            command=["python", script_path],
            args=args,
            completions=num_chunks,
            parallelism=kwargs.pop("parallelism", min(num_chunks, 10)),
            **kwargs
        )
        
        # Add completion index for chunk processing
        job_spec["spec"]["completionMode"] = "Indexed"
        
        return job_spec
    
    def save_job_yaml(self, job_spec: Dict[str, Any], output_path: str):
        """Save job specification to YAML file."""
        with open(output_path, 'w') as f:
            yaml.dump(job_spec, f, default_flow_style=False)
        print(f"Job YAML saved to {output_path}")
    
    def submit_job(self, job_spec: Dict[str, Any]) -> str:
        """
        Submit job to Kubernetes cluster.
        
        Args:
            job_spec: Job specification dictionary
            
        Returns:
            Job name
        """
        # Placeholder - to be implemented with kubernetes Python client
        raise NotImplementedError("Job submission to be implemented")


def generate_job_yaml(
    job_name: str,
    command: List[str],
    namespace: str = "default",
    **kwargs
) -> Dict[str, Any]:
    """
    Convenience function to generate a job YAML specification.
    
    Args:
        job_name: Name for the job
        command: Container command
        namespace: Kubernetes namespace
        **kwargs: Additional arguments passed to K8sJobManager
        
    Returns:
        Job specification dictionary
    """
    manager = K8sJobManager(namespace=namespace)
    return manager.generate_job_yaml(job_name, command, **kwargs)


def submit_job(job_spec: Dict[str, Any]) -> str:
    """
    Submit a job to Kubernetes cluster.
    
    Args:
        job_spec: Job specification dictionary
        
    Returns:
        Job name
    """
    manager = K8sJobManager()
    return manager.submit_job(job_spec)
