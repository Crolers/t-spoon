# 在 AWS EC2 实例上运行 TGStream

## 更新 SSH config


在 `~/.ssh/config` 中加入以下行：

```
Include ~/.ssh/config_flink
```

取得 EC2 实例的认证 `.pem` 文件后，
从以下文件夹得到 AWS console 的公共 IP 地址 `jobmanager_IP`:

```
$ ./update_flink_ssh_config path/to/identity_file.pem <jobmanager_IP>
```

## 启动/终止 集群

from the home directory:
从 home 目录下的 `jm_public_IP:8080` 获得 Flink 的 UI

```
$ source flinkrc
$ start_cluster.sh
$ ...
$ stop_cluster.sh
```

## 进行实验

在 home 目录下：

```
$ source flinkrc
```

以下命令将设置实验所用的环境变量，并更改执行实验脚本的目录：

```
$ cd t-spoon/launch_scripts
$ source evaluation_functions.sh
```

接下来进行实验：

```
# runs an experiment with a single transactional graph and a single
# stateful operator.
$ launch_series_1tg 1

# you can launch predefined suits:
$ launch_suite_series_1tg
$ ...
$ launch_suite_query
$ ...
# explore `evaluation_functions.sh` to have an overview of them.

# or you can launch the entire evaluation:
$ ./launch_evaluation my_first_evaluation
```

实验结果将输出至目录： `~/TGStream/launch_scripts/results`。仅进行单次实验时，例如对于实例`$ launch_series_1tg 1`，将得到如下单个 json 文件：

```
~/t-spoon/launch_scripts/results/series_1tg_1.json
```

而使用以下命令进行完整的实验时，

```
$ ./launch_evaluation my_first_evaluation
```
不同实验实例的结果将被划分进该目录下的多个文件夹： `~/t-spoon/launch_scripts/results/my_first_evaluation`.

## 结果收集及可视化

首先安装可视化所需的依赖包：

```
$ cd launch_scripts
$ ./install.sh
```

复制实验得到的结果：

```
$ cd t-spoon/launch_scripts
$ scp -r jm:~/t-spoon/launch_scripts/results ./results/last_results
```

对结果进行解析及可视化：

```
# parse data from experiment output and persists the result to json files
$ python parse_results.py ./results/last_results
# load the parsed results and generate plots
$ python plot_results.py ./results/last_results
```
