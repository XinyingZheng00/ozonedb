# Azure Experiment Driver

This is a driver for running experiments on Azure. It is designed to provision all resources needed on Azure, run the experiment, and then tear down all resources.

## Prerequisites

### Azure Cli

Install the Azure Cli

For macOS:

```bash
brew update && brew install azure-cli
```

For Linux:

```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

Login to Azure Cli by running:

```bash
az login
```

Export the environment variable `AZURE_SUBSCRIPTION_ID` with the subscription id of the Azure subscription you want to use. You can find the subscription id by following the instructions [here](https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id)

```bash
export AZURE_SUBSCRIPTION_ID=<subscription id>
```

Create ssh key for Azure virtual machines:

```bash
ssh-keygen -m PEM -t rsa -b 2048
```

This will create a public key file `~/.ssh/id_rsa.pub` which will be copied to the Azure virtual machines.

### Python

Install requirements, using a virtual environment is recommended:

```bash
python -m venv .venv
source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

### Config file

You can provide a config file to the scripts as you run them. The [example config file](./example-config.yaml), is a good starting point. The following fields must be set to your specific values:

```yaml
github_username: "mygithubusername"
github_token: "~/access_token.github"
airdb_branch: "main"
```

`~/access_token.github` should contain your GitHub access token. This is used to download the private airdb repository. The file should look like this:

```bash
❯ cat ~/access_token.github
ghp_myaccesstokenvalue
```

Also, it is recommended to create a specific resource group that only contains the resources created by this driver. This can be done by setting the `resource_group` field in the config file.

```yaml
resource_group:
  name: "airdb_azure_test"
```

Since deleting the resource group deletes all resources in the group, it is recommended to create a separate resource group for the driver.

## Usage

This is an example running an experiment on postgresql with the example config values. 

```bash
python create_azure_vms.py --config example-config.yaml
```
This will create the virtual machine instances on Azure with all the necessary resources.

Next, create a postgresql database.

```bash
python create_azure_postgres.py --config example-config.yaml
```

This will create a postgresql database, and add the necessary firewall rules to allow the virtual machines to connect to the database.

* Note: Because we need to add a new firewall rule for each vm, we need to run this script again after creating a new vm. If the server name is the same, it will not create a new server, but will only add the necessary firewall rules.

Now, we setup the database and vm for the experiment.

```bash
python setup_postgresql.py --config example-config.yaml
```

* Note: Again, if we add a new vm, we need to run this script again to add the necessary configuration to the new vm. Running this multiple times will not cause any data to be deleted in the database.

Next, we can load the data into the database. Running this will load the data and cache it so that we can reuse it for multiple experiments.

```bash
python load_postgresql_ycsb_t.py --config example-config.yaml
```

This will load the data into the database.

Finally, we can run the experiment.

```bash
python run_postgresql_ycsb_t.py --config example-config.yaml
```

This will run and download all the data for the experiments in the config file.

```yaml
ycsb_t:
  - workload: "workloada"
    operationcount: 10000
    threadcount: 1
  - workload: "workloadb"
    operationcount: 10000
    threadcount: 1
  - workload: "workloadc"
    operationcount: 10000
    threadcount: 1
  - workload: "workloadf"
    operationcount: 10000
    threadcount: 1
  - workload: "workloadd"
    operationcount: 10000
    threadcount: 1
```

This will run the workloads `a`, `b`, `c`, `f`, and `d` with 10000 operations and 1 thread. The results will be downloaded to the local machine. The results will be in the `results` directory.

```bash
❯ tree results
results
└── results_1vm
    ├── workloada
    │   └── postgresql
    │       ├── load_10000_t1_0.log
    │       ├── load_10000_t1_0.txt
    │       ├── run_10000_t1_0.log
    │       └── run_10000_t1_0.txt
    ├── workloadb
    │   └── postgresql
    │       ├── run_10000_t1_0.log
    │       └── run_10000_t1_0.txt
    ├── workloadc
    │   └── postgresql
    │       ├── run_10000_t1_0.log
    │       └── run_10000_t1_0.txt
    ├── workloadd
    │   └── postgresql
    │       ├── run_10000_t1_0.log
    │       └── run_10000_t1_0.txt
    └── workloadf
        └── postgresql
            ├── run_10000_t1_0.log
            └── run_10000_t1_0.txt

12 directories, 12 files
```

after running experiments, to run them again with 2 vms, change the config file to indicate 2 vms, and run the `create_azure_vms.py` script again. Then, run the `create_azure_postgres.py` script again to add the necessary firewall rules to the new vm. Then, run the `setup_postgresql.py` script again to add the necessary configuration to the new vm. Finally, run the `run_postgresql_ycsb_t.py` script again to run the experiments with 2 vms.

You do not want to run the `load_postgresql_ycsb_t.py` script again, as the data is already loaded into the database.

Finally, after running all the experiments we can delete all the resources.

Only run this when you are confident that you no longer need the resources, as this will delete all resources in the resource group.

```bash
python cleanup.py --config example-config.yaml
```

