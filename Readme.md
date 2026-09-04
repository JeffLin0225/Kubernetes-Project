# KEDA Kafka Autoscaling Lab (Serverless Event-Driven)

> 基於 **Kubernetes (KEDA v2.12)**、**Apache Kafka 3.4** 與 **.NET Worker** 的雲原生事件驅動自動水平擴展 (Auto-scaling) 實戰架構。  
> 透過即時監控 Kafka Consumer Group 的訊息堆積量 (Lag)，實現突破原生 CPU/Memory 限制的精準彈性擴展，並在負載歸零時自動冷卻縮容至零 (**Scale-to-Zero**)，達成極致的運算資源節省與完整可觀測性。

---

## 實作展示

[![完整實作展示影片](https://img.shields.io/badge/Click_to_Watch-實作展示影片(點此開啟)-blue?style=for-the-badge&logo=youtube)](https://pub-05c62739ac6f4499a3401b26d0e9faaf.r2.dev/video/KEDA_video.mp4)

![KEDA Scale-to-Zero 演示](KEDA_short.gif)

---

## 系統架構

系統運行於本機開發環境 (macOS + OrbStack)，透過 `host.docker.internal` 打通 K8s 叢集與宿主機 Docker 容器網路。架構整合了**核心擴縮容主鏈路**與**自動冷卻治理暨監控鏈路**：

```mermaid
flowchart TB
    %% --- 樣式定義 ---
    classDef kafka fill:#E65100,stroke:#FFE0B2,stroke-width:2px,color:#FFFFFF;
    classDef keda fill:#2E7D32,stroke:#C8E6C9,stroke-width:2px,color:#FFFFFF;
    classDef k8s fill:#1565C0,stroke:#BBDEFB,stroke-width:2px,color:#FFFFFF;
    classDef worker fill:#6A1B9A,stroke:#E1BEE7,stroke-width:2px,color:#FFFFFF;
    classDef obs fill:#C2185B,stroke:#F8BBD0,stroke-width:2px,color:#FFFFFF;
    classDef state fill:#455A64,stroke:#CFD8DC,stroke-width:2px,stroke-dasharray: 4 4,color:#FFFFFF;

    %% --- 1. 外部事件中樞 (Host Network) ---
    subgraph Host_Kafka["🖥️ Host 宿主機 Docker 網路環境"]
        direction TB
        Producer["fa:fa-paper-plane 外部事件生產者<br>Console / Script Producer"]
        Broker["fa:fa-server Apache Kafka Broker :9092<br>Listener: host.docker.internal"]
        TopicIn[("fa:fa-inbox source-topic<br>Partitions: 3 | Lag 監控來源")]
        TopicOut[("fa:fa-share-square router-topic<br>下游消費目標")]
    end

    %% --- 2. Kubernetes 叢集 ---
    subgraph K8s_Cluster["💻 Kubernetes Cluster (OrbStack)"]
        
        %% KEDA 控制與自動擴展層
        subgraph KEDA_Control["⚙️ KEDA 控制平面 (Namespace: keda)"]
            KedaOperator["fa:fa-robot KEDA Operator<br>事件控制器 & CRD 協調"]
            KedaMetrics["fa:fa-chart-bar KEDA Metrics Server<br>外部指標轉換 (External Metrics)"]
            ScaledObj["fa:fa-file-code ScaledObject: demo-kafka-scaler<br>lagThreshold: 5 | cooldown: 30s<br>minReplica: 0 | maxReplica: 5"]
        end

        %% K8s 原生伸縮層
        HPA["fa:fa-balance-scale 原生 HPA (k8s-hpa-demo-kafka-scaler)<br>動態依 External Metric 驅動 Pod 數量"]

        %% 工作負載層
        subgraph Workload_Zone["📦 應用工作負載 (Namespace: default)"]
            Deployment["fa:fa-layer-group Deployment: demo-worker<br>Image: keda-worker:v2"]
            ZeroState["fa:fa-moon 縮容至零 (Scale-to-Zero)<br>0 Replicas 待機無耗能"]:::state
            
            subgraph Active_Pods["動態彈性 Pod 群 (Replicas: 1 ~ 5)"]
                Pod1["fa:fa-cube .NET Worker Pod 1<br>Consumer Group: worker-group-id"]
                PodN["fa:fa-cubes .NET Worker Pod N<br>水平並行消費處理"]
            end
        end

        %% 可觀測性監控層
        subgraph Monitoring_Stack["📊 完整可觀測性 (Namespace: monitoring)"]
            Prometheus["fa:fa-database Prometheus Server<br>跨 Namespace 採集 指標"]
            Grafana["fa:fa-tv Grafana Dashboard :3000<br>LoadBalancer 零轉發直連"]
        end
    end

    %% --- 主流程連線 (數據流與擴容) ---
    Producer -->|"1. 拋送大量測試訊息"| TopicIn
    TopicIn --- Broker
    ScaledObj -.->|"2. 定義 Target Ref 與觸發規則"| KedaOperator
    KedaOperator -->|"3. 輪詢查詢 Consumer Lag (每 30s)"| Broker
    KedaOperator -->|"4. 暴露外部事件指標"| KedaMetrics
    KedaMetrics -->|"5. 提供 Lag 指標給 HPA"| HPA
    HPA -->|"6. 觸發擴容 Scale Up (0 -> N)"| Deployment
    Deployment --> Active_Pods
    Active_Pods ==|"7. 平行拉取並消費 (Consume)"|==> TopicIn
    Active_Pods ==|"8. 處理完成後寫入 (Produce)"|==> TopicOut

    %% --- 治理與可觀測性連線 (縮容與指標採集) ---
    TopicIn -.->|"A. 訊息處理完畢 (Lag = 0)"| KedaOperator
    KedaOperator -.->|"B. 冷卻倒數 30 秒 (cooldownPeriod)"| HPA
    HPA -.->|"C. 自動縮容至零 (Scale-to-Zero)"| ZeroState
    Prometheus -.->|"D. 採集 kube_deployment_status_replicas"| Deployment
    Grafana -->|"E. 即時繪製 Replicas / Lag 波動儀表板"| Prometheus

    %% --- 節點樣式套用 ---
    Producer:::kafka
    Broker:::kafka
    TopicIn:::kafka
    TopicOut:::kafka
    KedaOperator:::keda
    KedaMetrics:::keda
    ScaledObj:::keda
    HPA:::k8s
    Deployment:::worker
    Pod1:::worker
    PodN:::worker
    Prometheus:::obs
    Grafana:::obs
```

---

## 專案結構

```text
Kubernetes-KEDA/
├── KEDA/
│   └── deployment-keda.yml         # 定義 .NET Worker 的 Deployment 與 KEDA ScaledObject 擴縮容規則 (Lag 閾值/冷卻/極值)
├── Kafka/
│   └── docker-compose-kafka.yml    # 本地 Kafka 3.4 + Zookeeper 容器編排檔 (設定 host.docker.internal 雙通道監聽)
├── Prometheus-Grafana/
│   └── monitor-values.yml          # kube-prometheus-stack 客製 Helm Values (啟用 LoadBalancer、設定持久化與跨命名空間採集)
├── Installation.md                 # 基礎環境安裝指引 (Helm 安裝 KEDA、Prometheus/Grafana、Metrics Server 與 Kafka)
├── Usage.md                        # 操作手冊 (建立 Topic、監聽、部署應用、查看 HPA 與除錯指令速查)
├── Test_Report.md                  # 實作驗證報告 (實驗流程、核心成果總結與架構技術心得)
├── KEDA_short.gif                  # README 快速展示動態圖 (呈現 Pod 自動由 0 擴展並縮容之過程)
└── Readme.md                       # 專案總覽、系統架構圖與實作指南
```

---

## 核心設計理念與架構機制

### 1. 突破原生限制的精準擴容 (Precision Event-Driven Scaling)
* **傳統 HPA 局限**：傳統 HPA 依賴 CPU 或 Memory 使用率，面對 I/O 密集型或事件驅動工作負載時，通常等到 CPU 升高時佇列早已嚴重積壓。
* **KEDA Lag 驅動**：直接從 Kafka Broker 取得 Consumer Group 的真實積壓數量 (`Lag = High Watermark - Current Offset`)。本專案設定 `lagThreshold: 5`，當訊息積壓達 5 筆時即啟動 1 個 Pod，最多動態擴展至 5 個 Pods，實現「負載未至、算力先行」。

### 2. 極致成本優化：縮容至零 (Scale-to-Zero)
* 當佇列中的訊息全數處理完畢 (`Lag = 0`)，KEDA 將觸發冷卻計時器 (`cooldownPeriod: 30`)。
* 30 秒內若無新訊息湧入，Deployment 複本數將自動縮減至 `0`，釋放所有 Pod 佔用的記憶體與 CPU，達成與雲端 Serverless (如 AWS Lambda / Cloud Run) 相同的省成本效益。

### 3. 本地跨網路連通性 (Hybrid Host-Cluster Networking)
* 解決 macOS / OrbStack 環境下 K8s Pod 與宿主機 Docker 容器間的跨網路連線難題。
* 透過在 Kafka 設定 `PLAINTEXT_HOST://host.docker.internal:9092`，讓 K8s 叢集內的 KEDA Operator 與 .NET Worker 能無障礙連通宿主機 Kafka Broker。

---

## 快速開始與操作指南

### Step 1: 啟動外部 Kafka 基礎設施
```bash
# 啟動 Zookeeper 與 Kafka 容器
docker compose -f Kafka/docker-compose-kafka.yml up -d

# 進入 Kafka 容器建立測試用的來源與輸出 Topic (3 個 Partition)
docker exec -it kafka bash
kafka-topics --bootstrap-server localhost:9092 --create --topic source-topic --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic router-topic --partitions 3 --replication-factor 1
```

### Step 2: 安裝 KEDA 控制平面
```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace

# 確認 KEDA Operator 與 Metrics Server 正常啟動
kubectl get pods -n keda
```

### Step 3: 安裝可觀測性監控棧 (Prometheus + Grafana)
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  -f Prometheus-Grafana/monitor-values.yml

# 安裝 Metrics Server (K8s 資源指標)
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### Step 4: 部署工作負載與 KEDA 規則
```bash
# 部署 .NET Worker Deployment 與 ScaledObject
kubectl apply -f KEDA/deployment-keda.yml

# 監看擴縮容狀態 (預設初始 Pod 數量為 0)
kubectl get scaledobject -o wide
kubectl get hpa -w
kubectl get pods -w
```

### Step 5: 執行大量事件模擬測試
在宿主機或容器內大量推送訊息至 `source-topic`，觀察終端機與 Grafana 儀表板變化：
```bash
# 另開終端機監聽輸出 Topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic router-topic --from-beginning

# 查詢 Grafana 服務埠號並開啟儀表板 (預設帳密: admin / admin)
kubectl get svc -n monitoring
# 開啟瀏覽器訪問 http://localhost:3000
# 關鍵 PromQL: kube_deployment_status_replicas{deployment="demo-worker"}
```

---

## 實戰筆記與關鍵排錯 (Troubleshooting & Key Takeaways)

1. **Consumer Group 一致性陷阱**：
   `ScaledObject` 中宣告的 `consumerGroup` 必須與 .NET Worker 應用程式組態檔 (`appsettings.json` / 環境變數 `Kafka__ConsumerGroupId`) **完全一致**。若名稱不一致，KEDA 監控到的 Lag 將永遠為 0，導致擴容失敗。

2. **Offset 初始化問題 (Ghost Lag / Unknown Offset)**：
   新建或重建 Kafka Topic 後，若 Consumer 從未 Commit 過任何訊息，其 Offset 在 Kafka 內部為 `unknown`。此時 KEDA 可能無法計算有效數值。**解法**：在建立 Topic 後，先讓 Consumer 成功讀取並 Commit 至少一筆訊息，使 Offset 初始化為數值型態，KEDA 即可穩定判斷並正常縮容至 0。

3. **Partition 分配與測試發送技巧**：
   使用手動 `kafka-console-producer` 慢速發送訊息時，Kafka 預設的 Sticky Partition 機制會將訊息集中於單一 Partition。在驗證多 Pod 平行消費時，建議撰寫自動化 Script 連續快速發送大量訊息，讓資料均勻分散於 3 個 Partitions，精準驗證負載均衡效果。

---

## 常用維運指令速查

| 操作目標 | 指令 |
| :--- | :--- |
| **重啟 KEDA Operator** | `kubectl delete pod -n keda -l app=keda-operator` |
| **手動強制縮容 Pod** | `kubectl scale deployment demo-worker --replicas=0` |
| **檢視節點與 Pod 資源用量** | `kubectl top nodes` / `kubectl top pods` |
| **移除 KEDA 部署** | `kubectl delete -f KEDA/deployment-keda.yml` |
| **完整清理 Kafka 容器與磁碟區** | `docker compose -f Kafka/docker-compose-kafka.yml down -v` |
| **移除 KEDA Helm Chart** | `helm uninstall keda -n keda` |
