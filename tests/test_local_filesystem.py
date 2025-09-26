"""
Unit tests for the local filesystem implementation.
"""

import os
import unittest
import tempfile
import shutil
from pathlib import Path

from focus_ingest.filesystem.local import LocalFileSystem


class TestLocalFileSystem(unittest.TestCase):
    """Test cases for the LocalFileSystem class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.fs = LocalFileSystem()
        
        # Create some test files and directories
        os.makedirs(os.path.join(self.temp_dir, "dir1"))
        os.makedirs(os.path.join(self.temp_dir, "dir2", "subdir"))
        
        with open(os.path.join(self.temp_dir, "file1.txt"), "w") as f:
            f.write("File 1 content")
        
        with open(os.path.join(self.temp_dir, "dir1", "file2.txt"), "w") as f:
            f.write("File 2 content")
        
        with open(os.path.join(self.temp_dir, "dir2", "file3.txt"), "w") as f:
            f.write("File 3 content")
    
    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_list_files_simple(self):
        """Test that files are correctly listed with a simple pattern."""
        # Test with a simple path
        path = os.path.join(self.temp_dir, "file1.txt")
        
        # Convert to file URI
        uri = f"file://{Path(path).as_posix()}"
        
        result = self.fs.list_files(uri)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], uri)
    
    def test_list_files_directory(self):
        """Test that files are correctly listed in a directory."""
        # Test with a directory
        path = os.path.join(self.temp_dir, "dir1")
        
        # Convert to file URI
        uri = f"file://{Path(path).as_posix()}"
        
        result = self.fs.list_files(uri)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], f"file://{Path(os.path.join(path, 'file2.txt')).as_posix()}")
    
    def test_list_files_wildcard(self):
        """Test that files are correctly listed with a wildcard pattern."""
        # Test with a wildcard
        path = os.path.join(self.temp_dir, "**", "*.txt")
        
        # Convert to file URI
        uri = f"file://{Path(os.path.join(self.temp_dir)).as_posix()}/**/*.txt"
        
        result = self.fs.list_files(uri)
        self.assertEqual(len(result), 3)
        
        # Sort result for deterministic comparison
        result.sort()
        
        expected = [
            f"file://{Path(os.path.join(self.temp_dir, 'file1.txt')).as_posix()}",
            f"file://{Path(os.path.join(self.temp_dir, 'dir1', 'file2.txt')).as_posix()}",
            f"file://{Path(os.path.join(self.temp_dir, 'dir2', 'file3.txt')).as_posix()}"
        ]
        expected.sort()
        
        self.assertEqual(result, expected)
    
    def test_exists(self):
        """Test that existence is correctly determined."""
        # Test existing file
        path = os.path.join(self.temp_dir, "file1.txt")
        uri = f"file://{Path(path).as_posix()}"
        self.assertTrue(self.fs.exists(uri))
        
        # Test existing directory
        path = os.path.join(self.temp_dir, "dir1")
        uri = f"file://{Path(path).as_posix()}"
        self.assertTrue(self.fs.exists(uri))
        
        # Test non-existent file
        path = os.path.join(self.temp_dir, "nonexistent.txt")
        uri = f"file://{Path(path).as_posix()}"
        self.assertFalse(self.fs.exists(uri))
    
    def test_read_write_text(self):
        """Test that text is correctly read and written."""
        # Write text to a file
        path = os.path.join(self.temp_dir, "test_write.txt")
        uri = f"file://{Path(path).as_posix()}"
        content = "Test content for write"
        
        self.fs.write_text(uri, content)
        self.assertTrue(os.path.exists(path))
        
        # Read the file back
        result = self.fs.read_text(uri)
        self.assertEqual(result, content)
    
    def test_open_streams(self):
        """Test that streams are correctly opened."""
        # Write data to a file
        path = os.path.join(self.temp_dir, "test_stream.txt")
        uri = f"file://{Path(path).as_posix()}"
        content = b"Test content for stream"
        
        # Write using output stream
        with self.fs.open_output_stream(uri) as f:
            f.write(content)
        
        self.assertTrue(os.path.exists(path))
        
        # Read using input stream
        with self.fs.open_input_stream(uri) as f:
            result = f.read()
        
        self.assertEqual(result, content)
    
    def test_mkdirs_and_remove(self):
        """Test that directories are correctly created and removed."""
        # Create a directory
        path = os.path.join(self.temp_dir, "test_dir", "subdir")
        uri = f"file://{Path(path).as_posix()}"
        
        self.fs.mkdirs(uri)
        self.assertTrue(os.path.exists(path))
        
        # Write a file in the directory
        file_path = os.path.join(path, "test.txt")
        file_uri = f"file://{Path(file_path).as_posix()}"
        
        with self.fs.open_output_stream(file_uri) as f:
            f.write(b"Test content")
        
        self.assertTrue(os.path.exists(file_path))
        
        # Remove the directory recursively
        dir_uri = f"file://{Path(os.path.join(self.temp_dir, 'test_dir')).as_posix()}"
        
        self.fs.remove(dir_uri, recursive=True)
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, "test_dir")))
    
    def test_uri_handling(self):
        """Test that URI handling is correct."""
        # Test with file URI
        uri = f"file://{Path(self.temp_dir).as_posix()}/file1.txt"
        path = self.fs._uri_to_path(uri)
        self.assertEqual(path, os.path.join(self.temp_dir, "file1.txt"))
        
        # Test with local path
        uri = os.path.join(self.temp_dir, "file1.txt")
        path = self.fs._uri_to_path(uri)
        self.assertEqual(path, os.path.join(self.temp_dir, "file1.txt"))


if __name__ == "__main__":
    unittest.main()
