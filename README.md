# Air Quality Exporter for Prometheus

## 簡介

這是一個 Prometheus 的 exporter，用來將空氣品質的資料轉換成 Prometheus 可以讀取的格式。

空氣品質的資料來源是由[中正大學化生系 胡維平教授 實驗室](https://lab409chem.ccu.edu.tw/)所提供的資料。

## 使用方式

### 環境變數

使用者須透過環境變數來設定 exporter 的運作方式。以下是需要設定的環境變數：

-   `EXPORTER_PORT`: Exporter 監聽的端口號，預設為 `9100`。
-   `API_URL`: 空氣品質資料的 API URL。
-   `API_KEY`: 用於 API 認證的金鑰。
-   `API_UPDATE_INTERVAL_SEC`: 更新空氣品質資料的時間間隔（秒），預設為 `300` 秒。
-   `STATIONS_CONFIG_FILE`: 設定監測站的配置文件，預設為 `config.json`。
-   `LOG_LEVEL`: 設定日誌的等級，可選值有 `TRACE`, `DEBUG`, `INFO`, `SUCCESS`, `WARNING`, `ERROR`, `CRITICAL`，預設為 `WARNING`。

### 安裝與運行

1. clone 到本地：

    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. 安裝所需的依賴：

    ```sh
    pip install -r requirements.txt
    ```

3. 設定環境變數，可以在 `.env` 文件中設定：

    ```properties
    EXPORTER_PORT=[POR]
    API_URL=[API_URL]
    API_KEY=[API_KEY]
    API_UPDATE_INTERVAL_SEC=[UPDATE_INTERVAL_SEC]
    STATIONS_CONFIG_FILE=[STATIONS_CONFIG_FILE]
    LOG_LEVEL=[LOG_LEVEL]
    ```

4. 運行 exporter：
    ```sh
    python3 air_quality_exporter
    ```

### Prometheus 設定

在 Prometheus 的配置文件中添加以下設定來抓取 exporter 的數據：

```yaml
scrape_configs:
    - job_name: 'air_quality_prediction'
        scrape_interval: 300s

        - targets: ['localhost:9100']
```
