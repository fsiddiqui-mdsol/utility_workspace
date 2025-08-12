import requests
import json
import yaml
import argparse
import sys
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from kubernetes import client, config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FlinkJobInfo:
    job_id: str
    job_name: str
    status: str
    start_time: Optional[str]
    duration: Optional[str]
    checkpoint_dir: Optional[str]
    savepoint_dir: Optional[str]
    last_checkpoint: Optional[str]
    namespace: str
    cluster_name: str
    ingress_url: Optional[str]  # Add this new field

class FlinkJobStatusCollector:
    def __init__(self, namespace: str = "default", context: str = None):
        self.namespace = namespace
        self.context = context
        self.k8s_client = None
        self.networking_v1 = None
        self._setup_kubernetes_client()

    def _setup_kubernetes_client(self):
        """Setup Kubernetes client with specified context"""
        try:
            if self.context:
                config.load_kube_config(context=self.context)
            else:
                config.load_kube_config()
            self.k8s_client = client.CoreV1Api()
            self.networking_v1 = client.NetworkingV1Api()
            logger.info(f"Successfully connected to Kubernetes context: {self.context or 'default'}")
        except Exception as e:
            logger.error(f"Failed to setup Kubernetes client: {e}")
            sys.exit(1)    

    def _get_flink_clusters(self) -> List[Dict[str, Any]]:
        """Get all Flink clusters from both services and ingresses"""
        flink_clusters = []
        
        try:
            # First, try to find Flink clusters via ingresses
            ingress_clusters = self._get_flink_clusters_from_ingress()
            flink_clusters.extend(ingress_clusters)
            
            # Then, try to find via services (for internal access)
            # service_clusters = self._get_flink_clusters_from_services()
            
            # # Avoid duplicates by checking cluster names
            # existing_names = {cluster['name'] for cluster in flink_clusters}
            # for service_cluster in service_clusters:
            #     if service_cluster['name'] not in existing_names:
            #         flink_clusters.append(service_cluster)
            
            logger.info(f"Found {len(flink_clusters)} potential Flink clusters")
            return flink_clusters
            
        except Exception as e:
            logger.error(f"Failed to get Flink clusters: {e}")
            return []
    
    def _get_flink_clusters_from_ingress(self) -> List[Dict[str, Any]]:
        """Get Flink clusters exposed via ingress"""
        flink_clusters = []
        
        try:
            # Get all ingresses in the namespace
            ingresses = self.networking_v1.list_namespaced_ingress(namespace=self.namespace)
            
            for ingress in ingresses.items:
                ingress_name = ingress.metadata.name
                labels = ingress.metadata.labels or {}
                annotations = ingress.metadata.annotations or {}
                
                # Check if this looks like a Flink ingress
                is_flink_ingress = (
                    any(keyword in ingress_name.lower() for keyword in ['flink', 'jobmanager']) or
                    labels.get('app') == 'flink' or
                    labels.get('component') == 'jobmanager' or
                    any('flink' in str(v).lower() for v in labels.values()) or
                    any('flink' in str(v).lower() for v in annotations.values())
                )
                
                if not is_flink_ingress:
                    continue
                
                # Extract the ingress URL(s)
                ingress_urls = self._extract_ingress_urls(ingress)
                
                for url in ingress_urls:
                    flink_clusters.append({
                        'name': f"{ingress_name}",
                        'namespace': self.namespace,
                        'host': url['host'],
                        'port': url['port'],
                        'protocol': url['protocol'],
                        'ingress': ingress,
                        'type': 'ingress'
                    })
                    logger.info(f"Found Flink ingress: {url['protocol']}://{url['host']}:{url['port']}")
        
        except Exception as e:
            logger.warning(f"Failed to get Flink clusters from ingresses: {e}")
        
        return flink_clusters
    
    def _extract_ingress_urls(self, ingress) -> List[Dict[str, Any]]:
        """Extract URLs from ingress configuration"""
        urls = []
        
        try:
            # Check if ingress has rules
            if ingress.spec.rules:
                for rule in ingress.spec.rules:
                    host = rule.host
                    if not host:
                        continue
                    
                    # Determine protocol (check for TLS)
                    protocol = 'https' if ingress.spec.tls else 'http'
                    port = 443 if protocol == 'https' else 80
                    
                    # Check if there are specific paths for Flink
                    if rule.http and rule.http.paths:
                        for path in rule.http.paths:
                            # Look for Flink-specific paths or just use root
                            path_prefix = path.path if path.path != '/' else ''
                            urls.append({
                                'host': host,
                                'port': port,
                                'protocol': protocol,
                                'path_prefix': path_prefix
                            })
                    else:
                        urls.append({
                            'host': host,
                            'port': port,
                            'protocol': protocol,
                            'path_prefix': ''
                        })
            
            # Fallback: check load balancer ingress
            elif ingress.status.load_balancer and ingress.status.load_balancer.ingress:
                for lb_ingress in ingress.status.load_balancer.ingress:
                    host = lb_ingress.hostname or lb_ingress.ip
                    if host:
                        protocol = 'https' if ingress.spec.tls else 'http'
                        port = 443 if protocol == 'https' else 80
                        urls.append({
                            'host': host,
                            'port': port,
                            'protocol': protocol,
                            'path_prefix': ''
                        })
        
        except Exception as e:
            logger.warning(f"Error extracting ingress URLs: {e}")
        
        return urls
    
    def _get_flink_clusters_from_services(self) -> List[Dict[str, Any]]:
        """Get Flink clusters from services (original logic)"""
        flink_clusters = []
        
        try:
            # Get all services that might be Flink JobManagers
            services = self.k8s_client.list_namespaced_service(namespace=self.namespace)
            
            for service in services.items:
                # Look for Flink JobManager services (common patterns)
                service_name = service.metadata.name
                labels = service.metadata.labels or {}
                
                # Check if this looks like a Flink JobManager service
                if (any(keyword in service_name.lower() for keyword in ['flink', 'jobmanager']) or
                    labels.get('app') == 'flink' or
                    labels.get('component') == 'jobmanager'):
                    
                    # Try to determine the REST API port
                    rest_port = 8081  # default Flink REST port
                    for port in service.spec.ports or []:
                        if port.name in ['rest', 'ui', 'web'] or port.port in [8081, 8080]:
                            rest_port = port.port
                            break
                    
                    flink_clusters.append({
                        'name': service_name,
                        'namespace': self.namespace,
                        'host': f"{service_name}.{self.namespace}.svc.cluster.local",
                        'port': rest_port,
                        'protocol': 'http',
                        'service': service,
                        'type': 'service'
                    })
            
        except Exception as e:
            logger.warning(f"Failed to get Flink clusters from services: {e}")
        
        return flink_clusters
    
    def _get_job_manager_rest_endpoint(self, cluster: Dict[str, Any]) -> str:
        """Get the REST API endpoint for a Flink cluster"""
        protocol = cluster.get('protocol', 'http')
        host = cluster['host']
        port = cluster['port']
        path_prefix = cluster.get('path_prefix', '')
        
        # For standard HTTP/HTTPS ports, we might not need to include the port
        if (protocol == 'http' and port == 80) or (protocol == 'https' and port == 443):
            base_url = f"{protocol}://{host}"
        else:
            base_url = f"{protocol}://{host}:{port}"
        
        return f"{base_url}{path_prefix}"   
    
    def _make_flink_api_request(self, endpoint: str, path: str) -> Optional[Dict]:
        """Make a request to Flink REST API"""
        try:
            url = f"{endpoint}{path}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to connect to Flink REST API at {endpoint}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON response from {endpoint}: {e}")
            return None
    
    def _get_checkpoint_config(self, endpoint: str, job_id: str) -> Dict[str, Any]:
        """Get checkpoint configuration for a job"""
        checkpoint_info = {}
        
        # Get checkpoint config
        config_data = self._make_flink_api_request(endpoint, f"/jobs/{job_id}/checkpoints/config")
        # if config_data:
        #     checkpoint_info['checkpoint_dir'] = config_data.get('checkpoint_dir')
        #     checkpoint_info['savepoint_dir'] = config_data.get('externalized_checkpoint_dir')
        
        # Get latest checkpoint info
        checkpoints_data = self._make_flink_api_request(endpoint, f"/jobs/{job_id}/checkpoints")
        if checkpoints_data and 'latest' in checkpoints_data:
            latest = checkpoints_data['latest']
            if 'completed' in latest and latest['completed']:
                checkpoint_info['last_checkpoint'] = latest['completed'].get('external_path')
                checkpoint_info['checkpoint_dir'] = latest['completed'].get('external_path')
                if 'savepoint' in latest and latest['savepoint']!= None:
                    checkpoint_info['savepoint_dir'] = latest['savepoint'].get('external_path')
        return checkpoint_info
    
    def _format_duration(self, duration_ms: int) -> str:
        """Format duration from milliseconds to human readable format"""
        if not duration_ms:
            return "N/A"
        
        seconds = duration_ms // 1000
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24
        
        if days > 0:
            return f"{days}d {hours % 24}h {minutes % 60}m"
        elif hours > 0:
            return f"{hours}h {minutes % 60}m"
        elif minutes > 0:
            return f"{minutes}m {seconds % 60}s"
        else:
            return f"{seconds}s"
    
    def collect_job_status(self) -> List[FlinkJobInfo]:
        """Collect status information for all Flink jobs"""
        all_jobs = []
        flink_clusters = self._get_flink_clusters()
        
        if not flink_clusters:
            logger.warning("No Flink clusters found in the specified namespace")
            return all_jobs
        
        for cluster in flink_clusters:
            logger.info(f"Checking Flink cluster: {cluster['name']}")
            endpoint = self._get_job_manager_rest_endpoint(cluster)
            
            # Get jobs overview
            jobs_data = self._make_flink_api_request(endpoint, "/jobs/overview")
            if not jobs_data or 'jobs' not in jobs_data:
                logger.warning(f"No jobs found or unable to connect to cluster {cluster['name']}")
                continue
            
            for job in jobs_data['jobs']:
                job_id = job.get('jid')
                if not job_id:
                    continue
                
                if job.get('state') : #in ['RUNNING','FINISHED']:
                    # Get detailed job information
                    job_details = self._make_flink_api_request(endpoint, f"/jobs/{job_id}")
                    checkpoint_info = self._get_checkpoint_config(endpoint, job_id)
                else:
                    checkpoint_info = {}
                # Extract job information
                job_info = FlinkJobInfo(
                    job_id=job_id,
                    job_name=job.get('name', 'Unknown'),
                    status=job.get('state', 'Unknown'),
                    start_time=job.get('start-time'),
                    duration=self._format_duration(job.get('duration', 0)),
                    checkpoint_dir=checkpoint_info.get('checkpoint_dir'),
                    savepoint_dir=checkpoint_info.get('savepoint_dir'),
                    last_checkpoint=checkpoint_info.get('last_checkpoint'),
                    namespace=self.namespace,
                    cluster_name=cluster['name'],
                    ingress_url=endpoint if cluster.get('type') == 'ingress' else None  # Add ingress URL
                )
                
                all_jobs.append(job_info)
                logger.info(f"Found job: {job_info.job_name} ({job_info.status})")
        
        return all_jobs
    
    def print_job_status(self, jobs: List[FlinkJobInfo], output_format: str = "table"):
        """Print job status in specified format"""
        if not jobs:
            print("No Flink jobs found.")
            return
        
        if output_format == "json":
            job_dicts = []
            for job in jobs:
                job_dicts.append({
                    'job_id': job.job_id,
                    'job_name': job.job_name,
                    'status': job.status,
                    'start_time': job.start_time,
                    'duration': job.duration,
                    'checkpoint_dir': job.checkpoint_dir,
                    'savepoint_dir': job.savepoint_dir,
                    'last_checkpoint': job.last_checkpoint,
                    'namespace': job.namespace,
                    'cluster_name': job.cluster_name,
                    'ingress_url': job.ingress_url  # Add to JSON output

                })
            print(json.dumps(job_dicts, indent=2))
        
        elif output_format == "yaml":
            job_dicts = []
            for job in jobs:
                job_dicts.append({
                    'job_id': job.job_id,
                    'job_name': job.job_name,
                    'status': job.status,
                    'start_time': job.start_time,
                    'duration': job.duration,
                    'checkpoint_dir': job.checkpoint_dir,
                    'savepoint_dir': job.savepoint_dir,
                    'last_checkpoint': job.last_checkpoint,
                    'namespace': job.namespace,
                    'cluster_name': job.cluster_name,
                    'ingress_url': job.ingress_url  # Add to JSON output
                })
            print(yaml.dump(job_dicts, default_flow_style=False))
        
        else:  # table format
            print("\n" + "="*150)
            print(f"{'Job Name':<30} {'Status':<12} {'Duration':<15} {'Cluster':<20} {'Job ID':<32}")
            print("="*150)
            
            for job in jobs:
                print(f"{job.job_name[:29]:<30} {job.status:<12} {job.duration:<15} {job.cluster_name[:19]:<20} {job.job_id}")
            
            print("="*150)
            print(f"\nDetailed Information:")
            print("-" * 80)
            print("Seq, Name, Job Id, Status, Cluster, Namespace, Duration, Start Time, Checkpoint Dir, Savepoint Dir, Last Checkpoint,ingress URL")
            for i, job in enumerate(jobs, 1):
                print(f"{i}, {job.job_name}, {job.job_id},{job.status}, {job.cluster_name} , {job.namespace} , {job.duration} , {job.start_time or 'N/A'} , {job.checkpoint_dir or 'Not configured'} , {job.savepoint_dir or 'Not configured'} , {job.last_checkpoint or 'No checkpoints'},{job.ingress_url or 'N/A'}")
                # print(f"\n{i}. Job: {job.job_name}")
                # print(f"   Job ID: {job.job_id}")
                # print(f"   Status: {job.status}")
                # print(f"   Cluster: {job.cluster_name}")
                # print(f"   Namespace: {job.namespace}")
                # print(f"   Duration: {job.duration}")
                # print(f"   Start Time: {job.start_time or 'N/A'}")
                # print(f"   Checkpoint Dir: {job.checkpoint_dir or 'Not configured'}")
                # print(f"   Savepoint Dir: {job.savepoint_dir or 'Not configured'}")
                # print(f"   Last Checkpoint: {job.last_checkpoint or 'No checkpoints'}")

def main():
    parser = argparse.ArgumentParser(description='List Flink job status with checkpoint and savepoint information')
    parser.add_argument('--namespace', '-n', default='default', help='Kubernetes namespace (default: default)')
    parser.add_argument('--context', '-c', help='Kubernetes context to use')
    parser.add_argument('--format', '-f', choices=['table', 'json', 'yaml'], default='table', help='Output format (default: table)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        collector = FlinkJobStatusCollector(namespace=args.namespace, context=args.context)
        jobs = collector.collect_job_status()
        collector.print_job_status(jobs, args.format)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()