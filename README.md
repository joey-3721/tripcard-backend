# tripcard-backend

一个给 TripCard iOS 使用的自建后端示例，当前包含：

- `POST /v1/place-search` 地点搜索接口
- `GET /health` 健康检查
- `MySQL` 查询缓存
- `Docker` / `docker-compose` 部署

## 这是不是“完全自建”

这版是“后端自建 + MySQL 缓存 + 可部署在 NAS”。

它当前仍然会去请求外部地理数据源：

- Nominatim
- Photon

如果你后面要做到“地理数据也完全自建”，可以把 provider 切换成你自己部署的：

- 自建 Nominatim
- 自建 Photon / Pelias

但那会明显更重，尤其是全球数据，NAS 磁盘和内存压力会比较大。

## 本地运行

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 2778
```

## Docker 运行

```bash
cp .env.example .env
docker compose up -d --build
```

如果容器需要走代理访问外网，在 `.env` 里填写：

```env
HTTP_PROXY=http://你的NAS代理IP:端口
HTTPS_PROXY=http://你的NAS代理IP:端口
NO_PROXY=127.0.0.1,localhost
```

## MySQL 缓存

服务启动时会自动在 `travel` 库里创建缓存表：

- `place_search_cache`

缓存逻辑：

- 同一个请求先查 MySQL
- 命中且未过期：直接返回
- 未命中或已过期：重新请求外部 provider
- 新结果回写 MySQL

默认缓存 TTL 是 1 天：

```env
CACHE_TTL_SECONDS=86400
```

## 接口

### 健康检查

```bash
curl http://localhost:2778/health
```

### 搜索接口

```bash
curl -X POST http://localhost:2778/v1/place-search \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "仁川机场",
    "category": "transport",
    "scope": "all",
    "preferred_country_codes": ["JP"],
    "country_filter_code": null,
    "destination_context": {
      "trip_id": "demo-trip",
      "destinations": [
        {
          "name": "大阪",
          "country": "日本",
          "country_code": "JP"
        }
      ]
    },
    "language": "zh-CN",
    "limit": 12
  }'
```

## NAS 部署后访问地址

如果你的 NAS IP 是 `192.168.1.20`，默认端口不改，那么接口地址就是：

- `http://192.168.1.20:2778/health`
- `http://192.168.1.20:2778/v1/place-search`

## 群晖部署建议

建议把项目放在 `volume3`，例如：

```bash
/volume3/docker/tripcard-backend
```

`@docker` 这种目录通常是群晖套件自己创建和管理的，不需要强行放进去。你自己部署的项目可以单独放在 `volume3/docker/` 下，后面更好维护。

## 后续扩展建议

### 1. 你如果只是先跑通
保留现在这版就够了。

### 2. 你如果担心中国大陆访问外部数据不稳定
把这个服务部署到一台网络出口更稳定的机器上，或者后面替换成自建 Nominatim/Photon。

### 3. 你如果要“完全自建数据”
后续我建议单独加 provider 层，不要把数据下载逻辑硬塞进这个应用容器里。

原因是：

- 全球 OSM 数据很大
- 首次导入很慢
- 搜索索引和 API 服务最好拆开

如果你后面确定 NAS 配置，我可以再帮你出第二版：

- 哪个搜索引擎更适合你的 NAS
- 磁盘/内存大概要多少
- docker compose 怎么拆成多容器
