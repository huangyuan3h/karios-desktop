# Karios Desktop (Family Investment Analyzer) — Architecture & Requirements

> Document status: Draft v0.1  
> Last updated: 2025-12-20  
> Audience: Product/Engineering  
> Scope: Desktop app architecture + requirements baseline (no implementation yet)

---

## 1. Background & Vision

Karios Desktop is an **AI-first** personal finance and family investment analysis application. It runs as a **cross-platform desktop app** (macOS first, extend to Windows/Linux) and focuses on:

- Consolidating household investment data (positions, cashflows, assets, liabilities).
- Providing analytical insights (performance, allocation, risk, tax-aware views).
- Enabling a conversational workflow where the AI can explain, summarize, and guide decisions with transparency.

The initial technical stack intention:

- **UI**: Next.js + Tailwind CSS + shadcn-like component library
- **Desktop shell**: Tauri
- **API**: Split across **TypeScript (AI / orchestration)** and **Python (data / quant)**
- **AI**: Vercel AI SDK (server-side streaming endpoints)

---

## 2. Goals / Non-Goals

### 2.1 Goals

- **Local-first**: User data is stored locally by default; cloud sync is optional later.
- **AI-first UX**: Natural language as a primary interaction mode, with UI as structured “ground truth” views.
- **Explainable analysis**: AI outputs reference the underlying computed results and data sources where possible.
- **Modular backends**: Python for analytics/ETL/quant libs, TypeScript for AI endpoints and orchestration.
- **Cross-platform packaging**: Single desktop installer per platform, minimal setup for end users.

### 2.2 Non-Goals (v0)

- Brokerage account direct integrations (Plaid/IBKR/etc.) unless trivial; prefer file import first.
- Real-time trading/execution.
- Multi-user collaboration.
- Full accounting / bookkeeping.

---

## 3. Personas & Primary Use Cases

### 3.1 Personas

- **Household CFO**: Tracks family assets, rebalancing, long-term planning.
- **Analytical investor**: Wants factor exposure, drawdown, scenario analysis.
- **Occasional user**: Imports statements quarterly, wants a clean summary.

### 3.2 Primary Use Cases (v0)

- **Data ingestion**
  - Import CSV/OFX/QIF exports (bank/broker).
  - Manual entry for assets/liabilities (real estate, mortgage, etc.).
- **Portfolio overview**
  - Current allocation (asset class, region, sector).
  - Performance (time-weighted / money-weighted, benchmark optional).
- **Risk & scenario**
  - Volatility, max drawdown, stress tests (simple scenarios first).
- **AI copilot**
  - “What changed this month?”, “Why is performance down?”, “What is my USD exposure?”
  - Generate a monthly report (markdown/pdf later).

---

## 4. Requirements

### 4.1 Functional Requirements

- **R1 Data model**
  - Support accounts, transactions, holdings, instruments, FX rates.
  - Track asset/liability categories and custom tags.
- **R2 Import**
  - CSV import with mapping UI.
  - De-duplication and idempotent re-import.
- **R3 Analysis**
  - Valuation by date, P&L decomposition (at least realized/unrealized).
  - Allocation breakdowns and time series charts.
- **R4 AI chat**
  - Streaming responses.
  - Tool-calling style actions (e.g., “compute allocation”, “generate report”).
  - Citation-like references to computed artifacts (tables/charts/queries).
- **R5 Privacy controls**
  - Local storage encryption option.
  - Explicit consent for any data leaving the device (AI requests).

### 4.2 Non-Functional Requirements

- **NFR1 Performance**
  - Large CSV imports should complete within reasonable time (target: < 30s for typical household scale).
  - Interactive UI should remain responsive; heavy computations off the UI thread.
- **NFR2 Reliability**
  - Crash-safe writes; recoverable state.
  - Deterministic analytics for same inputs.
- **NFR3 Security**
  - API keys are never stored in plaintext if avoidable.
  - Local services are bound to `127.0.0.1` and protected by an auth token.
- **NFR4 Maintainability**
  - Clear module boundaries; testable services.
  - Telemetry optional and opt-in only.

---

## 5. Architecture Options (Tradeoff Analysis)

We evaluate three feasible architectures for Next.js + Tauri + Python + TS.

### Option A — Tauri-managed Local Services (Recommended)

**Idea**: Tauri (Rust) launches and supervises two local backends:

- **Python service** for data/analytics (e.g., FastAPI over HTTP, or gRPC).
- **Node/TypeScript service** for AI endpoints using **Vercel AI SDK** (HTTP with streaming).

The Next.js UI runs in the Tauri WebView and talks to these services via `fetch` to `http://127.0.0.1:<port>`.

**Pros**

- Best separation of concerns (UI / AI / analytics).
- Keeps Vercel AI SDK in its natural environment (Node runtime).
- Python can freely use scientific/finance libraries.
- Scales to more services (e.g., background jobs) without redesign.

**Cons**

- Packaging complexity: shipping Python runtime + dependencies; shipping Node runtime (or bundling) depending on approach.
- Need robust service lifecycle management (ports, crash restart, logs).

### Option B — Pure Tauri Commands (Rust as API Boundary)

**Idea**: UI calls `tauri.invoke()` commands; Rust executes logic and returns results. Python is embedded via PyO3 or spawned subprocess. AI is also proxied through Rust.

**Pros**

- Simplifies networking (no local HTTP server).
- Strong security boundary; easy to restrict surface area.

**Cons**

- Harder to integrate Vercel AI SDK (Node-centric).
- Rust becomes a bottleneck for orchestration and JSON schema evolution.
- Streaming chat UX is more complex (requires custom event streaming).

### Option C — Single Node Backend + Python as Library/Worker

**Idea**: Node backend provides API; Python runs as a worker process for heavy compute; UI talks only to Node.

**Pros**

- One API surface; easier for UI integration.
- Good fit for AI and streaming.

**Cons**

- Still need Python packaging; plus IPC design between Node and Python.
- Over-centralizes orchestration in Node; some analytics might want direct Python API.

---

## 6. Recommended Architecture (Option A)

### 6.1 High-level Diagram

```
┌──────────────────────────┐
│        Tauri Shell        │
│ (Rust, lifecycle, storage │
│   bridges, security)      │
└───────────┬──────────────┘
            │
            │ starts/supervises
            ▼
┌──────────────────────────┐      HTTP (localhost)      ┌──────────────────────────┐
│      Next.js UI (WebView) │ ─────────────────────────▶ │   AI Service (TS/Node)   │
│ Tailwind + shadcn-like UI │ ◀───────────────────────── │ Vercel AI SDK streaming  │
└───────────┬──────────────┘                              └──────────────────────────┘
            │
            │ HTTP (localhost)
            ▼
┌──────────────────────────┐
│  Data/Quant Service (Py)  │
│ ETL, analytics, risk, FX  │
└───────────┬──────────────┘
            │
            ▼
┌──────────────────────────┐
│   Local Data Store        │
│ SQLite / DuckDB + files   │
└──────────────────────────┘
```

### 6.2 Component Responsibilities

#### Desktop Shell (Tauri / Rust)

- Start/stop services (Python + Node), manage ports and logs.
- Provide secure storage bridge (e.g., token storage, file dialogs).
- Enforce local security:
  - Bind only to `127.0.0.1`
  - Random ephemeral ports
  - Per-session auth token injected to UI

#### UI (Next.js)

- App shell and pages: dashboard, imports, portfolio, reports, settings, chat.
- UI state management and caching (query layer).
- Minimal domain logic; delegate heavy computations to services.

#### AI Service (TypeScript / Node)

- Vercel AI SDK endpoints (chat, tools).
- Tool orchestration:
  - Call Data/Quant service to compute canonical results.
  - Summarize and explain results with a consistent prompt policy.
- Ensure data minimization:
  - Send only necessary aggregates to LLM when possible.

#### Data/Quant Service (Python)

- Ingestion pipelines and normalization.
- Analytics computations (returns, allocation, risk metrics).
- Provide deterministic outputs with versioned schemas.

#### Local Data Store

- **Primary**: SQLite for structured entities and transactions.
- **Optional**: DuckDB for fast analytical queries/time series (evaluate later).
- File vault for imports and derived artifacts.

---

## 7. AI-first Product Design Principles

- **Two-lane UX**:
  - Lane A: structured UI (tables/charts) as ground truth.
  - Lane B: AI chat that can reference Lane A outputs and propose actions.
- **Tool-first**: AI answers should prefer calling tools (compute/query) rather than guessing.
- **Traceability**: Store “analysis artifacts” (queries, computed tables) and attach them to AI responses.
- **Safety & privacy**:
  - “Local compute first” for analysis.
  - “Explicit send” for LLM calls, with preview of what will be sent.

### 7.1 Context acquisition (mobile apps + desktop sources)

Most user context may originate from mobile apps (e.g., Xueqiu) and desktop tools (e.g., TradingView). Screenshots are a fallback, not the primary path.

Design principle:

- Prefer **structured or semi-structured ingestion** (links, exports, alerts) over images.
- Use images only when there is no export/share path; treat image extraction as a versioned pipeline with human review when needed.

---

## 8. API Contracts & Data Flow (Conceptual)

### 8.1 Data/Quant API (Python)

Core endpoints/actions (names TBD):

- `import_transactions(file, mapping) -> import_summary`
- `get_portfolio_snapshot(date) -> snapshot`
- `get_performance(range, benchmark?) -> series`
- `get_allocation(date, dimensions) -> table`
- `get_risk_metrics(range, scenario_set?) -> metrics`

### 8.2 AI API (TypeScript)

- `POST /chat` (streaming)
  - Accepts conversation + optional context references
  - Calls tools as needed
  - Returns streaming text + tool results metadata

### 8.3 Data Flow Example: “Why is my portfolio down this month?”

1. UI sends question to AI service.
2. AI service calls Python for:
   - monthly P&L by asset, FX attribution, large movers.
3. AI service summarizes results and returns:
   - streaming narrative + references to computed tables.
4. UI renders narrative and attaches expandable “evidence” panels.

---

## 9. Security, Privacy, and Compliance (Baseline)

- **Local-only default**: no outbound network unless user enables AI provider usage.
- **API key handling**:
  - Store in OS keychain via Tauri secure storage plugin (preferred).
- **Local service hardening**:
  - Loopback binding only, random ports, auth token required.
  - Disable CORS except the embedded UI origin.
- **Data encryption at rest (optional v0.2)**
  - Encrypt the database and vault with a user passphrase.

---

## 10. Packaging & Runtime Considerations

Key decision: how to ship Python and Node.

- **Python packaging**: bundle a Python runtime + wheels, or use embedded distribution per platform.
- **Node/TS packaging**:
  - Bundle into a standalone executable (preferred) OR ship node runtime.
  - Keep the AI service small and stable.

Tauri will be the single entry point that bootstraps both services.

---

## 11. Proposed Repository Structure (Target)

```
/
  apps/
    desktop-ui/         # Next.js UI (Tauri WebView)
    ai-service/         # TS/Node service using Vercel AI SDK
  services/
    data-sync-service/  # Python data service (Postgres)
  packages/
    shared/             # shared types/schemas, prompt policies
  docs/
    architecture-and-requirements.md
```

---

## 12. Testing Strategy (Design-level)

- **Python**
  - Unit tests for calculations and import normalization.
  - Golden test datasets (small CSV fixtures) for regression.
- **TypeScript AI service**
  - Contract tests for tool interfaces (mock Python service).
  - Prompt policy tests (snapshot style) to ensure consistent tool usage.
- **UI**
  - Component tests for import mapping UI and core dashboards.
  - Integration tests with mocked services.

---

## 13. Open Questions / Decisions Needed

Please confirm/decide the following to finalize v0 architecture:

1. **Offline AI**: AI requires network is acceptable for v0 (local analysis first, AI as an add-on).
2. **AI provider**: Gemini 3 and/or OpenAI 5.2 (selectable provider).
3. **Data store**: SQLite-only for v0 (DuckDB deferred; see trigger conditions below).
4. **Target OS**: Single-machine (this Mac) is sufficient for v0.
5. **Import formats**: Primary sources are likely screenshots (TradingView, broker apps) + Python data libraries (e.g., AKShare). Exact formats are TBD.
6. **Encryption**: Not a priority for v0; revisit later.

---

## 14. Database Recommendation (Local-first)

### 14.1 Requirements-driven constraints

- The dataset size for a household is typically not huge, but queries can be analytical (time series, grouping, pivot-like views).
- We need to store **raw artifacts** (screenshots, files) and **structured normalized data** (accounts/transactions/holdings).
- We want a simple, robust local storage story that is easy to backup/migrate.

### 14.2 Recommended approach (v0)

**SQLite as the system of record**, plus a local file vault for large artifacts.

- **SQLite**
  - Store canonical structured entities (accounts, instruments, transactions, holdings, FX rates, tags).
  - Use **FTS5** for local full-text search (e.g., notes, extracted text from screenshots).
  - Use WAL mode and crash-safe migrations.
- **File vault (filesystem)**
  - Store screenshot images, PDFs, original CSVs, and derived artifacts (JSON exports, intermediate tables).
  - SQLite stores pointers + hashes + metadata for integrity and de-duplication.

This keeps v0 architecture simple and reliable, while remaining extensible.

### 14.3 When to add DuckDB (v0.2+ or when needed)

Add **DuckDB** if/when we hit any of these:

- Frequent large time-series scans (multi-year daily history across many instruments).
- Need for fast ad-hoc analytics (multi-dimensional aggregation, window functions) beyond SQLite comfort.
- Desire to store/read Parquet efficiently for derived datasets.

**Hybrid model**: SQLite remains the system of record; DuckDB is used as an analytics engine over derived tables (and/or Parquet) generated from SQLite snapshots.

**Decision (v0)**: SQLite-only (lowest complexity). Re-evaluate DuckDB only after we observe real query bottlenecks.

---

## 15. Data Ingestion Strategy (Screenshots / TradingView / Python libs)

Given imports are likely “messy” and image-based, we should treat ingestion as a **pipeline with provenance** rather than a one-off import.

### 15.1 Source categories

- **A. Structured sources**
  - CSV exports (broker/bank), OFX/QIF where available
  - Python data libraries (e.g., AKShare) for prices, fundamentals, FX
- **B. Semi-structured sources**
  - Screenshots (TradingView charts, broker app positions/history)
  - PDFs (statements)
- **C. Manual inputs**
  - Real estate valuation, private assets, custom liabilities

### 15.2 Proposed pipeline (v0)

1. **Capture**
   - User imports a file or drags a screenshot into the app.
   - Store the original artifact into the file vault; compute SHA-256 for de-duplication.
2. **Extraction**
   - For screenshots/PDFs, run OCR to extract text.
   - Use a multimodal LLM (Gemini is a strong candidate here) to convert the visual/text content into a **typed JSON schema** (positions/transactions tables).
3. **Normalization**
   - Python service validates types, units, currencies, dates; resolves instruments; applies mapping rules.
   - Save normalized records into SQLite, linked back to the artifact and extraction version.
4. **Review & mapping UI**
   - A mapping screen lets the user confirm columns, currencies, and instrument matches.
   - Show diffs when re-importing the same source (idempotent behavior).

**Note**: The exact screenshot extraction approach (multimodal-first vs OCR-first) is intentionally deferred until implementation, where we can test against real user samples and compare quality/cost.

### 15.3 Why this fits an AI-first approach

- Screenshots are inherently unstructured; the best v0 path is **LLM-assisted extraction + deterministic validation**.
- Python remains the “truth layer”: even if extraction is fuzzy, the stored normalized dataset is consistent.

### 15.4 Risks and mitigations

- **Extraction errors**
  - Mitigation: strict schema validation, confidence scoring, and mandatory user review for low confidence.
- **Vendor/model variability**
  - Mitigation: keep extraction prompts and schemas versioned; store raw OCR text and model metadata.
- **Data gaps**
  - Mitigation: allow partial imports + manual corrections; treat missing fields explicitly.

---

## 16. Next Steps (Suggested)

- Define the **minimum dataset** and schemas (accounts, transactions, holdings).
- Write an **ADR** (Architecture Decision Record) for packaging approach (Python/Node).
- Draft an MVP feature list and milestone plan (2–4 week slices), prioritizing ingestion and portfolio overview.

---

## 17. Trading Execution & Market Data Collection (Exploratory)

This section is intentionally exploratory. The goal is to keep the architecture flexible while we learn which brokers/markets and libraries we will actually use.

### 17.1 What is `easytrader` and where it fits

`easytrader` is a Python project that provides:

- **Client automation trading**: placing/canceling orders and querying balances/positions by controlling broker/THS (TongHuaShun) desktop clients.
- **Optional official interface**: documentation mentions support for broker official quant interfaces (e.g., miniQMT).
- **Remote mode**: the trading client can be operated remotely (useful when the trading environment must be Windows).

**Key implication**: the client-automation part typically depends on **a running Windows desktop trading client** and is sensitive to UI changes. It is powerful for rapid experiments but is inherently more brittle than official APIs.

### 17.2 How to integrate trading safely in our architecture

We should isolate trading into a dedicated adapter layer in the Python service:

- Define a stable internal interface:
  - `get_accounts()`, `get_positions()`, `get_balance()`
  - `place_order()`, `cancel_order()`, `get_orders()`, `get_trades()`
- Implement providers as adapters:
  - `EasyTraderAdapter` (client automation)
  - `MiniQMTAdapter` (official API)
  - Other providers (IBKR/Futu/etc.) if needed later

This prevents UI/AI code from depending on a single library.

### 17.3 macOS constraint (v0)

Because v0 runs on **this Mac**, using `easytrader` client automation may require one of:

- **A. Windows VM on the same Mac** (run the broker client + `easytrader` inside the VM)
- **B. Remote Windows host** (run broker client + `easytrader` remotely; our Mac app talks to it over a secured channel)
- **C. Use an official cross-platform API** (preferred when available)

We should treat the execution stack as pluggable and decide later based on your actual broker and market.

### 17.4 UX principle for AI-first trading

Even in an AI-first product, trading actions must be explicit and reversible where possible:

- AI can propose an order and explain why.
- UI must present a structured order ticket (symbol, side, price, qty, account) and require confirmation.
- Store an order intent log for debugging and post-mortems.

### 17.5 Note on Ping An Securities (retail) feasibility

For Ping An Securities retail accounts, a stable public programmatic trading API is not clearly documented as broadly available.
In practice, automation options usually fall into one of these buckets:

- **Use official in-app automation**: conditional orders / grid-style tools provided by the broker app (lowest engineering cost, limited programmability).
- **Client automation on Windows**: run the broker/THS desktop client on Windows and automate it (higher flexibility, higher brittleness).
- **Official quant gateway (if available to your account tier)**: some brokers provide QMT-like gateways, which are generally more robust than GUI automation.

Given v0 is macOS single-machine, if we need true programmatic order placement, we should assume a **Windows execution environment** (VM or remote host) unless Ping An provides an official gateway we can access.

### 17.6 Staged plan for Ping An (mobile-first → programmatic)

Given the user primarily trades via the **mobile app**, and can accept **conditional orders first**, we should stage execution capabilities:

- **Stage 0 (v0): Decision support only**
  - The desktop app focuses on analysis, risk checks, and generating a _proposed_ action plan.
  - Output is a structured "order recipe" (symbol, side, price rule, qty rule, triggers, validity) that the user manually configures as a conditional order in the broker app.
  - Store these recipes and compare them with later outcomes for learning and reporting.
- **Stage 1 (semi-automated): Assisted execution**
  - Add templates for common conditional-order patterns (stop-loss, take-profit, rebalance bands).
  - Add post-trade reconciliation: ingest screenshots/PDF/exports to confirm what actually happened.
- **Stage 2 (programmatic execution): Pluggable execution gateway**
  - Introduce a dedicated execution adapter (see 17.2) and route order placement through it.
  - Prefer an official gateway if Ping An provides one to the account tier; otherwise, use a Windows execution host as a contained subsystem.

This approach delivers value immediately while avoiding brittle automation until we have a clear, supportable execution path.

---

## 18. Context Collection for AI Chat (Without Screenshot-First)

This section defines how we can feed high-quality context into the model when most information lives inside mobile apps (e.g., Xueqiu) and desktop tools (e.g., TradingView).

### 18.1 Context sources and preferred capture methods

- **TradingView (desktop/web)**
  - Prefer: alerts (webhook/email) and any exportable data (e.g., indicator values, watchlists, CSV where available).
  - Secondary: copy/paste text summaries or shareable chart links.
  - Fallback: screenshots of charts.
- **Mobile apps (Xueqiu, broker apps)**
  - Prefer: share sheet (share link/text), copy text/notes, exported statements (PDF/CSV) if available.
  - Secondary: periodic “manual snapshot” forms (holdings summary, cash balance) entered quickly.
  - Fallback: screenshots.
- **Files**
  - PDFs, CSVs, and any reports stored locally should be ingested as first-class artifacts.

### 18.2 Recommended v0 mechanism: "Context Collector"

Add a dedicated feature that collects context as **artifacts** and makes them available to chat tools.

Artifacts can be:

- `url` (shared link), `text` (copied content), `file` (pdf/csv/image), `table` (structured rows), `note` (user annotation)

Each artifact stores:

- source, timestamp, tags, confidence, extracted text (if any), and a stable hash for de-duplication.

### 18.3 Practical workflows (low friction)

- **Share → desktop app**: user shares a link or text from mobile to the desktop collector (via a lightweight inbox mechanism to be designed).
- **Paste a link**: user pastes a TradingView/Xueqiu link; the app stores it and (when permitted) extracts title/summary.
- **Alerts inbox**: TradingView alerts can be routed to an inbox (email/webhook) and saved as structured events.

### 18.4 Why this beats screenshot-first

- Lower cost (less OCR/vision inference).
- Higher accuracy (structured payloads).
- Better traceability (URLs/events can be re-queried and re-parsed).

### 18.5 Screenshot policy (fallback)

When screenshots are necessary:

- Always store the original image.
- Extract text via OCR, then optionally use a multimodal model to produce typed JSON.
- Require human review for low confidence or high-impact actions.
