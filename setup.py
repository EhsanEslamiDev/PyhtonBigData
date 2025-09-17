"""
Installation and Setup Guide
From Krish Naik's Big Data Course

Quick setup instructions for the Python Big Data course.
"""

import subprocess
import sys
import os

def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("⚠️  Warning: Python 3.8+ is recommended for this course")
        return False
    else:
        print("✅ Python version is compatible")
        return True

def check_java():
    """Check if Java is installed (required for Spark)."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, stderr=subprocess.STDOUT)
        if result.returncode == 0:
            print("✅ Java is installed")
            print(f"Java version: {result.stderr.split()[2].strip('\"')}")
            return True
        else:
            print("❌ Java not found")
            return False
    except FileNotFoundError:
        print("❌ Java not installed")
        print("Please install Java 8 or higher for Apache Spark")
        return False

def install_requirements():
    """Install required packages."""
    print("\n=== Installing Python packages ===")
    
    # Essential packages for basic functionality
    basic_packages = [
        'numpy>=1.21.0',
        'pandas>=1.5.0',
        'matplotlib>=3.5.0',
        'jupyter>=1.0.0'
    ]
    
    # Optional packages for advanced features
    optional_packages = [
        'pyspark>=3.3.0',
        'seaborn>=0.11.0',
        'plotly>=5.0.0',
        'sqlalchemy>=1.4.0'
    ]
    
    try:
        # Install basic packages first
        print("Installing basic packages...")
        for package in basic_packages:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
        
        print("\n✅ Basic packages installed successfully!")
        
        # Try to install optional packages
        print("\nInstalling optional packages...")
        for package in optional_packages:
            try:
                print(f"Installing {package}...")
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            except subprocess.CalledProcessError:
                print(f"⚠️  Failed to install {package} (optional)")
        
        print("\n✅ Installation complete!")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Installation failed: {e}")
        return False
    
    return True

def verify_installation():
    """Verify that key packages are installed."""
    print("\n=== Verifying Installation ===")
    
    packages_to_check = {
        'numpy': 'numpy',
        'pandas': 'pandas', 
        'matplotlib': 'matplotlib',
        'jupyter': 'jupyter',
        'pyspark': 'pyspark'
    }
    
    for package_name, import_name in packages_to_check.items():
        try:
            __import__(import_name)
            print(f"✅ {package_name} is available")
        except ImportError:
            print(f"❌ {package_name} is not available")

def create_virtual_environment():
    """Guide for creating virtual environment."""
    print("\n=== Virtual Environment Setup ===")
    print("It's recommended to use a virtual environment:")
    print()
    print("1. Create virtual environment:")
    print("   python -m venv venv")
    print()
    print("2. Activate virtual environment:")
    print("   # On Windows:")
    print("   venv\\Scripts\\activate")
    print("   # On macOS/Linux:")
    print("   source venv/bin/activate")
    print()
    print("3. Install requirements:")
    print("   pip install -r requirements.txt")
    print()

def setup_guide():
    """Complete setup guide."""
    print("🚀 Python Big Data Course Setup")
    print("=" * 40)
    
    # Check Python version
    python_ok = check_python_version()
    
    # Check Java
    java_ok = check_java()
    
    if not python_ok:
        print("\nPlease upgrade Python to version 3.8 or higher")
        return
    
    # Virtual environment guide
    create_virtual_environment()
    
    # Ask user if they want to install packages
    response = input("\nDo you want to install required packages now? (y/n): ")
    
    if response.lower() in ['y', 'yes']:
        install_requirements()
        verify_installation()
    else:
        print("\nTo install packages later, run:")
        print("pip install -r requirements.txt")
    
    print("\n=== Next Steps ===")
    print("1. Navigate to any module directory")
    print("2. Run the Python scripts: python script_name.py")
    print("3. Start Jupyter for interactive notebooks: jupyter lab")
    print("4. Check the exercises/ directory for practice problems")
    print("\nHappy learning! 🎓")

if __name__ == "__main__":
    setup_guide()