# Alma Relay

[English](README.md) | 简体中文

**[Alma](https://alma.now) 远程访问**的自托管中继：在任何地方用手机上的 Alma
控制电脑上的 Alma。不用端口转发，不用 VPN，中间也没有第三方。

```
桌面端 Alma ──主动发起 WSS──▶  你的中继  ◀──HTTPS/WSS── 手机上的 Alma
```

桌面端主动连接中继，并一直保持这条连接。手机把请求发给中继，中继再通过这条连接
转给桌面端，HTTP、流式响应（SSE）和 WebSocket 都支持。开启端到端加密（Alma
默认开启）后，中继只能看到密文。它不往磁盘写任何东西，也接触不到你的连接码。

Alma 自带一个官方中继（`relay.alma.now`）。如果你希望流量只经过自己的设施、想要
离自己更近的节点，或者不想依赖别人的服务器，可以自己部署一个。

## 部署

任选一种，几分钟就能完成。

### 方案 A：Cloudflare Workers（免费，不需要服务器）

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/yetone/alma-relay)

或者克隆后用命令行部署：

```bash
git clone https://github.com/yetone/alma-relay && cd alma-relay
npm install
npx wrangler deploy        # 输出 https://alma-relay.<你的子域>.workers.dev
```

**Workers 免费套餐**就能跑：用的是 SQLite 版 Durable Object 和 WebSocket
休眠 API，一个空闲的配对几乎不产生费用。TLS 和 `*.workers.dev` 域名都是现成的。
要用自己的域名，取消 `wrangler.jsonc` 里 `routes` 的注释即可。

### 方案 B：Docker，自动配置 HTTPS（任意 VPS）

需要一台开放了 80/443 端口的服务器，并把一个域名解析到它。

```bash
git clone https://github.com/yetone/alma-relay && cd alma-relay
cp .env.example .env       # 设置 RELAY_DOMAIN=relay.example.com
docker compose up -d
```

[Caddy](https://caddyserver.com) 会自动申请并续期 Let's Encrypt 证书。部署好后
中继地址就是 `https://relay.example.com`。

### 方案 C：放在你自己的反向代理后面

```bash
docker run -d --restart unless-stopped -p 127.0.0.1:8787:8787 ghcr.io/yetone/alma-relay
# 不用 Docker 的话（Node ≥ 20）：
npx github:yetone/alma-relay
```

然后把你的 HTTPS 域名反代到 `127.0.0.1:8787`。注意要允许 WebSocket，并关闭响应
缓冲（nginx 和 Caddy 的配置示例见[英文 README](README.md#option-c-behind-your-own-reverse-proxy)）。

用 `curl https://relay.example.com/healthz` 检查是否正常运行。

## 在 Alma 里使用

1. 在桌面端打开 **设置 → 远程访问**。
2. 点 **使用自建中继**，填入中继的 `https://` 地址，再点 **使用此中继**。
3. 如果中继设置了密钥（见下文），把它粘贴到 **Alma 账号 → 手动粘贴令牌**。
4. 用手机上的 Alma 扫描配对二维码。

手机端不需要任何设置，配对链接里已经带着中继地址。切换中继之前配对过的手机，
需要重新扫码。

## 配置

所有配置都通过环境变量设置：Docker 写在 `.env` 里；Cloudflare 上，密钥类用
`wrangler secret put NAME`，普通配置写在 `wrangler.jsonc` 的 `vars` 里。
每一项都是可选的。

| 变量 | 作用 |
| --- | --- |
| `RELAY_REGISTRATION_KEY` | 共享密钥。桌面端必须带上它才能注册，在 Alma 里粘贴到令牌处即可。 |
| `RELAY_TOKEN_SECRET` | 改用按用户签发的令牌，适合一个中继给团队共用（见下文）。设置后优先于 `RELAY_REGISTRATION_KEY`。 |
| `RELAY_TOKEN_MESSAGE` | 桌面端没有有效令牌时收到的提示，比如告诉对方去哪儿领令牌（默认 `A valid relay token is required`）。 |
| `PORT` / `HOST` | 监听地址（Node 版默认 `8787` / `0.0.0.0`）。 |
| `MAX_BODY_MB` | 请求体上限，单位 MB（Node 版默认 128；Cloudflare 版对协议 2 的桌面端默认 100，对旧桌面端固定为 24）。 |
| `MAX_BUFFERED_MB` | Node 版：同时缓存在内存里的请求体总量上限，单位 MB（默认 512），超出返回 `503`。发给协议 2 桌面端的请求体是流式转发的，不计入。 |
| `MAX_AGENTS` | Node 版：同时在线的桌面端上限（默认 10000）。 |
| `MAX_REQUESTS_PER_AGENT` / `MAX_SOCKETS_PER_AGENT` | Node 版：每台桌面端的并发 HTTP 请求 / WebSocket 上限（默认各 256）。 |
| `MAX_CONNECTIONS` | Node 版：进程的 TCP 连接上限（默认不限）。 |
| `FLOW_STREAM_WINDOW_KB` | 协议 3 的桌面端：每条流最多领先 relay 多少 KB（默认 1024）。这样慢的手机只会拖住自己那条流。见[协议 3](docs/PROTOCOL.md#protocol-3)。 |
| `FLOW_HIGH_WATER_MB` / `FLOW_LOW_WATER_MB` | Node 版：某个手机端积压到这么多 MB 时暂停读取对应桌面端，降到低水位以下再恢复。默认 8 / 1。对协议 3 的桌面端只是兜底。见[流控](docs/PROTOCOL.md#flow-control-node-edition)。 |
| `STALL_TIMEOUT_S` | 手机端这么多秒读不走任何数据就断开（Node 版）；协议 2 的桌面端这么多秒不接收上传数据也会被断开（两个版本都有）。默认 60。见[流控](docs/PROTOCOL.md#flow-control-node-edition)。 |
| `RELAY_PROTOCOL` | 向桌面端提供的最高协议版本（默认 `3`）。设为 `2` 关闭[按流流控](docs/PROTOCOL.md#protocol-3)；设为 `1` 连[协议 2](docs/PROTOCOL.md#protocol-2)（流式上传、二进制帧）也关掉。 |
| `LOCATION_HINT` | 仅 Cloudflare：指定隧道所在区域，例如 `apac`、`enam`、`weur`。默认会建在离桌面端近的地方；桌面端通过代理、离手机很远时，可以用它把隧道放到手机附近。改了之后，每台桌面端下次重连时会换到新区域的隧道；还没重连的，手机也照样找得到。 |
| `TURN_URLS` / `TURN_SECRET` / `TURN_TTL` | 为手机实时查看桌面浏览器（WebRTC）签发 TURN 凭据。即 `TURN_PROVIDER=coturn`（默认）。 |
| `TURN_PROVIDER` | `coturn`（默认）或 `cloudflare`——`/turn` 从哪个 TURN 服务签发凭据（见下文）。 |
| `TURN_KEY_ID` / `TURN_KEY_API_TOKEN` | Cloudflare 的 TURN 密钥（控制台 → Calls），`TURN_PROVIDER=cloudflare` 时必填。 |
| `TURN_API_URL` | 覆盖 Cloudflare 凭据 API 的地址。仅供测试使用，平时不要设置。 |
| `APNS_KEY_FILE` 或 `APNS_KEY`，以及 `APNS_KEY_ID`、`APNS_TEAM_ID`、`APNS_TOPIC` | 手机通知（见下文）。 |
| `FCM_SERVICE_ACCOUNT_FILE` 或 `FCM_SERVICE_ACCOUNT` | 通过 Firebase 给 Android 手机发通知（见下文）。 |

### 谁能使用你的中继

不设密钥时，任何知道中继地址的人都能在上面开隧道，但他们依然连不到*你的*
电脑，因为那需要你那台电脑 256 位的连接码。个人自用通常这样就够了。不想让陌生人
用你的服务器，就设置 `RELAY_REGISTRATION_KEY`。

如果要给团队共用一个中继，并且能单独停用某个人，就设置 `RELAY_TOKEN_SECRET`，
给每个人签发一个令牌：

```bash
RELAY_TOKEN_SECRET=… npm run mint-token -- alice 365     # 用户 ID、有效天数
# Docker：docker compose exec relay node scripts/mint-token.mjs alice
```

令牌是标准的 HS256 JWT（`aud: "alma-relay"`、`sub`、`exp`），用任何 JWT 库都能
签发。更换密钥会让所有已签发的令牌一起失效。

### 手机通知

手机锁屏后，iOS App 没法保持连接，所以“回复完成”“Alma 有问题要问你”这类提醒
要走 Apple 推送，由中继代桌面端发出。Apple 只接受给 App 签名的那个开发者团队的
密钥发来的推送，**所以自托管中继没法给官方 Alma iOS App 发通知**，其余功能都
不受影响。如果你自己编译 iOS App，可以给中继配上你自己的密钥：

```bash
APNS_KEY_FILE=/path/AuthKey_XXXX.p8 APNS_KEY_ID=XXXX APNS_TEAM_ID=YYYY APNS_TOPIC=your.bundle.id
# Cloudflare：wrangler secret put APNS_KEY < AuthKey_XXXX.p8（以及 APNS_KEY_ID、APNS_TEAM_ID）
```

Android 手机走 Firebase Cloud Messaging。把 Firebase 项目的服务账号密钥交给中继
（Firebase 控制台 → 项目设置 → 服务账号 → 生成新的私钥）：

```bash
FCM_SERVICE_ACCOUNT_FILE=/path/service-account.json
# Cloudflare：wrangler secret put FCM_SERVICE_ACCOUNT < service-account.json
```

和 APNs 一样，Firebase 只会把推送送到这个密钥所属项目的 App。只配了其中一种密钥时，
另一种手机会收到 `ApnsUnsupported` / `FcmUnsupported`。

两种密钥都没有配置时，中继会回复 `unsupported`，桌面端之后就不会再请求推送。

### 实时浏览器画面用的 TURN

手机可以通过 WebRTC 实时查看桌面端的浏览器。在大多数网络环境下这需要一台 TURN
服务器。Docker 部署方案里已经带了 coturn：

```bash
# 写进 .env：
TURN_SECRET=<一串足够长的随机字符>
TURN_URLS=stun:relay.example.com:3478,turn:relay.example.com:3478?transport=udp,turn:relay.example.com:3478?transport=tcp

docker compose --profile turn up -d
```

防火墙需要放行 UDP/TCP `3478`和 UDP `49160-49200`。用 Cloudflare 部署的话，
把 coturn 跑在别的机器上，再给 Worker 配上 `TURN_URLS` / `TURN_SECRET`。中继
只负责签发短期凭据，音视频数据不会经过它。

#### Cloudflare 的 TURN

不想自己维护 coturn 的话，中继也可以改为从 Cloudflare 的托管 TURN
（`turn.cloudflare.com`，anycast；与 Realtime SFU 搭配使用免费，否则按出站
流量计费）签发凭据。Cloudflare 不讲 coturn 的 static-auth-secret 那套——TURN
密钥不能直接当 `turn.cloudflare.com` 的凭据用，只能通过他们的凭据 API 兑换，
所以 Cloudflare 上的中继是指向这个 API 的：

1. 在控制台创建 TURN 密钥（Calls → TURN keys），保管好其中的 **Token ID** 和
   **API token**，它们只应存在于服务端。
2. 给 Worker 配上：

   ```bash
   TURN_PROVIDER=cloudflare
   TURN_KEY_ID=<Token ID>
   TURN_KEY_API_TOKEN=<API token>
   ```

中继会为每次请求兑换一个短期凭据，用一个不可反推的 id 标注它（用于 Cloudflare
的用量统计），然后在 `/turn` 里返回 API 给的 `iceServers` 以及自己要求的
ttl。兑换失败返回 `502`，和未配置时的 `404` 区分开。Node 版同样支持这个
provider，变量相同。

## 安全

- **端到端加密。** 开启端到端加密后（新配对默认开启），手机和桌面端会在隧道里
  完成一次带认证的 ECDH 握手，协商出 AES-256-GCM 会话密钥。连接码参与了密钥
  推导，所以运营中继的人也无法冒充任何一方或解密通信。会话具有前向保密性。
- **请使用 Alma 0.4.329 或更新版本。** 从这个版本起，开启端到端加密的桌面端会
  拒绝所有明文请求，运营中继的人无法直接调用桌面端的 API。更早的版本仍会响应由
  中继自己发出的明文请求。
- **中继上没有可偷的东西。** 中继按 `sha256(路由令牌)` 转发，路由令牌又是从
  连接码单向推导出来的。连接码、令牌和通信内容都不会写入磁盘或日志。
- **一定要通过 HTTPS 提供服务。** 端到端加密已经保护了内容，TLS 还能保护路由
  元数据。
- 连接码相当于你电脑的密码。在远程访问设置里重新生成连接码，就能让所有已配对的
  手机失效。

## 开发

```bash
npm install
npm start                    # 在 :8787 启动 Node 版
npx wrangler dev             # 在本地启动 Cloudflare 版

npm test                     # 对 Node 版跑端到端测试
npm run test:cloudflare      # 对 `wrangler dev` 跑同一套测试
node test/e2e.mjs https://relay.example.com   # 对已部署的中继做冒烟测试
```

协议细节见 [docs/PROTOCOL.md](docs/PROTOCOL.md)。

## 许可证

[MIT](LICENSE)
