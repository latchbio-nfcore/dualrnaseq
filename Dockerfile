# DO NOT CHANGE
from 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:fe0b-main

workdir /tmp/docker-build/work/

shell [ \
    "/usr/bin/env", "bash", \
    "-o", "errexit", \
    "-o", "pipefail", \
    "-o", "nounset", \
    "-o", "verbose", \
    "-o", "errtrace", \
    "-O", "inherit_errexit", \
    "-O", "shift_verbose", \
    "-c" \
]
env TZ='Etc/UTC'
env LANG='en_US.UTF-8'

arg DEBIAN_FRONTEND=noninteractive

# Latch SDK
# DO NOT REMOVE
run pip install latch==2.46.6
run mkdir /opt/latch
run apt-get update && apt-get install -y default-jre-headless

# Install Mambaforge
run apt-get update --yes && \
    apt-get install --yes curl && \
    curl \
        --location \
        --fail \
        --remote-name \
        https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh && \
    `# Docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos` \
    bash Mambaforge-Linux-x86_64.sh -b -p /opt/conda -u && \
    rm Mambaforge-Linux-x86_64.sh

# Set conda PATH
env PATH=/opt/conda/bin:$PATH
RUN conda config --set auto_activate_base false

# Build conda environment
copy environment.yml /opt/latch/environment.yaml
run mamba env create \
    --file /opt/latch/environment.yaml \
    --name nf-core-dualrnaseq-1.0.0
env PATH=/opt/conda/envs/nf-core-dualrnaseq-1.0.0/bin:$PATH


# Copy workflow data (use .dockerignore to skip files)

copy . /root/

# Latch nextflow workflow entrypoint
# DO NOT CHANGE

run ln -s /root/.latch/bin/nextflow /root/nextflow
run ln -s /root/.latch/.nextflow /root/.nextflow


# Latch workflow registration metadata
# DO NOT CHANGE
arg tag
# DO NOT CHANGE
env FLYTE_INTERNAL_IMAGE $tag

workdir /root
