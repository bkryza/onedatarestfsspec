#!/usr/bin/env python3
"""Basic usage example for OnedataFSSpec."""

import fsspec
import os
from pathlib import Path


def main():
    """Demonstrate basic OnedataFSSpec usage."""
    
    # Configuration - replace with your actual values
    onezone_host = os.environ.get('ONEDATA_ONEZONE_HOST', 'https://datahub.egi.eu')
    token = os.environ.get('ONEDATA_TOKEN')
    
    if not token:
        print("Error: ONEDATA_TOKEN environment variable must be set")
        print("Example: export ONEDATA_TOKEN='your_access_token_here'")
        return
    
    # Create filesystem instance
    print(f"Connecting to Onedata at {onezone_host}...")
    fs = fsspec.filesystem(
        'onedata',
        onezone_host=onezone_host,
        token=token
    )
    
    try:
        # List available spaces
        print("\n=== Available Spaces ===")
        spaces = fs.ls('/')
        for space in spaces:
            print(f"  {space}")
        
        if not spaces:
            print("  No spaces found")
            return
            
        # Use the first available space for examples
        example_space = spaces[0]
        print(f"\nUsing space '{example_space}' for examples...")
        
        # List files in the space
        print(f"\n=== Files in space '{example_space}' ===")
        try:
            files = fs.ls(f'/{example_space}/', detail=True)
            for file_info in files[:10]:  # Show first 10 files
                file_type = "DIR" if file_info['type'] == 'directory' else "FILE"
                size = file_info.get('size', 0)
                name = Path(file_info['name']).name
                print(f"  {file_type:4} {size:>10} {name}")
            
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more files")
                
        except Exception as e:
            print(f"  Error listing files: {e}")
        
        # Example: Create a test file
        test_file_path = f'/{example_space}/fsspec_test_file.txt'
        test_content = "Hello from OnedataFSSpec!\nThis is a test file.\n"
        
        print(f"\n=== Writing test file ===")
        try:
            with fs.open(test_file_path, 'w') as f:
                f.write(test_content)
            print(f"  Created: {test_file_path}")
            
            # Read it back
            with fs.open(test_file_path, 'r') as f:
                read_content = f.read()
            print(f"  Content: {repr(read_content[:50])}...")
            
            # Get file info
            info = fs.info(test_file_path)
            print(f"  Size: {info['size']} bytes")
            print(f"  Type: {info['type']}")
            
        except Exception as e:
            print(f"  Error with test file: {e}")
        
        # Example: Check file operations
        print(f"\n=== File Operations ===")
        try:
            exists = fs.exists(test_file_path)
            print(f"  File exists: {exists}")
            
            is_file = fs.isfile(test_file_path)
            print(f"  Is file: {is_file}")
            
            size = fs.size(test_file_path) if exists else 0
            print(f"  File size: {size} bytes")
            
        except Exception as e:
            print(f"  Error checking file operations: {e}")
        
        # Example: Directory operations
        test_dir_path = f'/{example_space}/fsspec_test_dir'
        print(f"\n=== Directory Operations ===")
        try:
            fs.makedirs(test_dir_path, exist_ok=True)
            print(f"  Created directory: {test_dir_path}")
            
            is_dir = fs.isdir(test_dir_path)
            print(f"  Is directory: {is_dir}")
            
        except Exception as e:
            print(f"  Error with directory operations: {e}")
        
        # Cleanup (optional)
        print(f"\n=== Cleanup ===")
        try:
            if fs.exists(test_file_path):
                fs.rm(test_file_path)
                print(f"  Removed: {test_file_path}")
            
            if fs.exists(test_dir_path):
                fs.rmdir(test_dir_path)
                print(f"  Removed: {test_dir_path}")
                
        except Exception as e:
            print(f"  Error during cleanup: {e}")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()