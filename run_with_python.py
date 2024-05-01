import subprocess

# Path to the Rust executable
executable_path = '/root/.cargo/bin/delta_inventory'
table_path = ''

command = [executable_path, table_path]
# Using subprocess.run() to execute the Rust program
# This captures the output as well
result = subprocess.run(command, capture_output=True, text=True)

# Print the standard output and error from the Rust program
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
