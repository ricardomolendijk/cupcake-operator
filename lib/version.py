"""
Version management module - Handles Kubernetes version validation and upgrade paths
"""
import logging
import re
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


class Version:
    """Represents a Kubernetes version"""
    
    def __init__(self, version_string: str):
        """
        Parse a Kubernetes version string
        Accepts: "1.27.4", "v1.27.4", "1.27"
        """
        # Remove 'v' prefix if present
        version_string = version_string.lstrip('v')
        
        # Parse version components
        parts = version_string.split('.')
        
        if len(parts) < 2:
            raise ValueError(f"Invalid version format: {version_string}")
        
        self.major = int(parts[0])
        self.minor = int(parts[1])
        self.patch = int(parts[2]) if len(parts) > 2 else 0
        
    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __repr__(self):
        return f"Version({self})"
    
    def __eq__(self, other):
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    def __lt__(self, other):
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __le__(self, other):
        return self < other or self == other
    
    def __gt__(self, other):
        return not self <= other
    
    def __ge__(self, other):
        return not self < other
    
    def minor_version(self) -> str:
        """Return minor version string (e.g., '1.27')"""
        return f"{self.major}.{self.minor}"
    
    def full_version(self) -> str:
        """Return full version string with patch"""
        return str(self)


def get_current_cluster_version() -> Optional[Version]:
    """
    Get the current cluster version by querying the API server
    Returns None if unable to determine
    """
    try:
        from kubernetes import client
        v1 = client.VersionApi()
        version_info = v1.get_code()
        
        # version_info.git_version is like "v1.27.4"
        version_str = version_info.git_version
        return Version(version_str)
    except Exception as e:
        logger.error(f"Failed to get cluster version: {e}")
        return None


def calculate_upgrade_path(current: Version, target: Version) -> List[Version]:
    """
    Calculate the upgrade path from current to target version.
    
    Kubernetes upgrade rules:
    - Can upgrade to next minor version (1.27 → 1.28)
    - Can skip patch versions within same minor (1.27.1 → 1.27.9)
    - CANNOT skip minor versions (1.27 → 1.29 requires 1.27 → 1.28 → 1.29)
    
    Returns list of versions to upgrade through, including target
    """
    if current >= target:
        logger.warning(f"Current version {current} is already >= target {target}")
        return []
    
    path = []
    
    # If same minor version, jump directly to target (patch upgrade)
    if current.minor == target.minor:
        logger.info(f"Patch version upgrade: {current} → {target}")
        path.append(target)
        return path
    
    # Calculate minor version steps
    minor_diff = target.minor - current.minor
    
    if minor_diff == 1:
        # Direct minor version upgrade
        logger.info(f"Single minor version upgrade: {current} → {target}")
        path.append(target)
    else:
        # Multi-step upgrade required
        logger.warning(f"Multi-step upgrade required: {current} → {target} ({minor_diff} minor versions)")
        
        # Build intermediate versions
        for minor in range(current.minor + 1, target.minor + 1):
            # For intermediate steps, use the latest known patch version
            # or default to .0 if we don't have patch info
            if minor == target.minor:
                # Final step: use target version
                intermediate = target
            else:
                # Intermediate step: suggest latest patch for that minor
                # In practice, kubeadm will use the latest available patch
                intermediate = Version(f"{current.major}.{minor}.0")
            
            path.append(intermediate)
            logger.info(f"  Step {len(path)}: upgrade to {intermediate}")
    
    return path


def validate_version_string(version: str) -> Tuple[bool, str]:
    """
    Validate a version string format
    Returns (is_valid, message)
    """
    try:
        v = Version(version)
        
        # Kubernetes version constraints
        if v.major != 1:
            return False, f"Only Kubernetes 1.x versions are supported (got {v.major}.x)"
        
        if v.minor < 20:
            return False, f"Kubernetes {v} is too old (minimum supported: 1.20)"
        
        if v.minor > 31:
            return False, f"Kubernetes {v} is not yet released or supported"
        
        return True, f"Version {v} is valid"
        
    except ValueError as e:
        return False, f"Invalid version format: {e}"


def get_upgrade_warnings(current: Version, target: Version) -> List[str]:
    """
    Get warnings about the upgrade path
    Returns list of warning messages
    """
    warnings = []
    
    if current >= target:
        warnings.append(f"Target version {target} is not newer than current {current}")
        return warnings
    
    # Check for major version differences
    if target.major != current.major:
        warnings.append(f"Major version change detected: {current.major} → {target.major}")
    
    # Check for large minor version jumps
    minor_diff = target.minor - current.minor
    if minor_diff > 3:
        warnings.append(
            f"Large version jump: {minor_diff} minor versions. "
            f"This will require {minor_diff} sequential upgrades."
        )
    
    # Check for skipping LTS versions
    # Kubernetes has Long-Term Support (LTS) for certain versions
    # Currently 1.23, 1.24, 1.25, 1.26, 1.27, 1.28 are LTS candidates
    
    # Warn about deprecated APIs
    if current.minor <= 21 and target.minor >= 22:
        warnings.append(
            "Upgrading from 1.21 or earlier to 1.22+: "
            "Several APIs have been removed (beta versions of common resources). "
            "Ensure all manifests use stable API versions."
        )
    
    if current.minor <= 24 and target.minor >= 25:
        warnings.append(
            "Upgrading to 1.25+: PodSecurityPolicy has been removed. "
            "Migrate to Pod Security Standards before upgrading."
        )
    
    if current.minor <= 25 and target.minor >= 26:
        warnings.append(
            "Upgrading to 1.26+: Several beta APIs have been removed. "
            "Review the release notes for breaking changes."
        )
    
    return warnings


def get_next_minor_version(current: Version) -> Version:
    """Get the next minor version (e.g., 1.27.4 → 1.28.0)"""
    return Version(f"{current.major}.{current.minor + 1}.0")


def is_patch_upgrade(current: Version, target: Version) -> bool:
    """Check if this is only a patch version upgrade"""
    return current.major == target.major and current.minor == target.minor


def format_upgrade_path_message(path: List[Version]) -> str:
    """Format upgrade path for logging"""
    if not path:
        return "No upgrade needed"
    
    if len(path) == 1:
        return f"Direct upgrade to {path[0]}"
    
    path_str = " → ".join(str(v) for v in path)
    return f"Multi-step upgrade path: {path_str} ({len(path)} steps)"
