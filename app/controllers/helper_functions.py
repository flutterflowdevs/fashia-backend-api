import os
import pwd
import logging

logger = logging.getLogger(__name__)

def get_container_user_info():
    """Get comprehensive user information in container environment"""
    user_info = {}
    
    try:
        # Get current user ID and group ID
        user_info['uid'] = os.getuid()
        user_info['gid'] = os.getgid()
        
        # Try to get username from password database
        try:
            user_info['username'] = pwd.getpwuid(os.getuid()).pw_name
        except (KeyError, ImportError):
            user_info['username'] = f"user_{os.getuid()}"  # Fallback
        
        # Get effective user ID (if different)
        user_info['euid'] = os.geteuid()
        user_info['egid'] = os.getegid()
        
        # Get all groups the user belongs to
        try:
            import grp
            groups = os.getgroups()
            group_names = []
            for gid in groups:
                try:
                    group_names.append(grp.getgrgid(gid).gr_name)
                except KeyError:
                    group_names.append(f"group_{gid}")
            user_info['groups'] = group_names
        except ImportError:
            user_info['groups'] = [f"group_{gid}" for gid in os.getgroups()]
            
    except Exception as e:
        user_info['error'] = f"Failed to get user info: {e}"
    
    return user_info

def get_container_environment_info():
    """Get container environment information"""
    env_info = {}
    
    # Current working directory and file system info
    env_info['current_directory'] = os.getcwd()
    env_info['database_path_env'] = os.getenv('DATABASE_PATH', 'Not set')
    
    # Check if we're running in a container
    env_info['in_container'] = os.path.exists('/.dockerenv')
    
    # Check common container environment variables
    container_vars = [
        'HOSTNAME', 'ECS_CONTAINER_METADATA_URI', 'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
        'AWS_EXECUTION_ENV', 'container', 'DOCKER_CONTENT_TRUST'
    ]
    
    for var in container_vars:
        env_info[var] = os.getenv(var, 'Not set')
    
    return env_info