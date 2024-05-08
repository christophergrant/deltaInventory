curl https://sh.rustup.rs -sSf | sh -s -- -y # install rust

. "$HOME/.cargo/env" # source the env

cargo install --git https://github.com/christophergrant/deltaInventory

# now the program can be run at /root/.cargo/bin/delta_profiler
