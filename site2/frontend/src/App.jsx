import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';
import { apiGet, apiPost, clearToken, setToken } from './api';

const numberFormat = new Intl.NumberFormat('ko-KR');

function formatKrw(value) {
  if (value === null || value === undefined) return '-';
  return `${numberFormat.format(Math.round(value))}원`;
}

function formatPct(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return `${(value * 100).toFixed(digits)}%`;
}

function formatTimeLabel(value) {
  if (!value) return '-';
  return value.slice(5, 16).replace('T', ' ');
}

function formatShortDate(value) {
  if (!value) return '-';
  return value.slice(2);
}

function StatusBadge({ value }) {
  const lowered = String(value || '').toUpperCase();
  const tone = lowered.includes('SUCCESS') ? 'ok' : lowered.includes('ERROR') ? 'error' : 'muted';
  return <span className={`badge badge-${tone}`}>{value || '-'}</span>;
}

function App() {
  const [authReady, setAuthReady] = useState(false);
  const [loginRequired, setLoginRequired] = useState(true);
  const [authenticated, setAuthenticated] = useState(false);
  const [loginForm, setLoginForm] = useState({ username: '', password: '' });
  const [loginError, setLoginError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const [summary, setSummary] = useState(null);
  const [account, setAccount] = useState(null);
  const [historyData, setHistoryData] = useState([]);
  const [latestPortfolio, setLatestPortfolio] = useState(null);
  const [etfData, setEtfData] = useState({ limit_70: [], limit_100: [], counts: {} });
  const [selectedCode, setSelectedCode] = useState('');
  const [chartModalOpen, setChartModalOpen] = useState(false);
  const [mobileTab, setMobileTab] = useState('overview');
  const [priceSeries, setPriceSeries] = useState({ code: '', name: '', series: [] });
  const [searchWord, setSearchWord] = useState('');

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    checkAuthStatus();
  }, []);

  useEffect(() => {
    if (!authenticated) return;
    loadDashboardBase();
  }, [authenticated]);

  useEffect(() => {
    if (!authenticated || account === null || account === undefined) return;
    loadPortfolio(account);
  }, [authenticated, account]);

  useEffect(() => {
    if (!authenticated || !selectedCode) return;
    loadPriceSeries(selectedCode);
  }, [authenticated, selectedCode]);

  function openChartForCode(code) {
    if (!code) return;
    setSelectedCode(code);
    setChartModalOpen(true);
  }

  async function checkAuthStatus() {
    try {
      const status = await apiGet('/api/auth/status');
      setLoginRequired(Boolean(status.login_required));
      setAuthenticated(Boolean(status.authenticated));
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setAuthReady(true);
      setLoading(false);
    }
  }

  async function loadDashboardBase() {
    setLoading(true);
    setError('');
    try {
      const [summaryPayload, etfPayload] = await Promise.all([
        apiGet('/api/summary'),
        apiGet('/api/etf/categories')
      ]);

      setSummary(summaryPayload);
      setEtfData(etfPayload);

      const accounts = (summaryPayload.accounts || []).map((item) => item.account);
      setAccount((prev) => {
        if (accounts.length === 0) return null;
        if (prev === null || prev === undefined) return accounts[0];
        return accounts.includes(prev) ? prev : accounts[0];
      });

      const defaultCode = etfPayload.limit_70?.[0]?.code || etfPayload.limit_100?.[0]?.code || '';
      if (defaultCode) {
        setSelectedCode(defaultCode);
      }
    } catch (requestError) {
      if (requestError.status === 401) {
        clearToken();
        setAuthenticated(false);
      }
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }

  async function loadPortfolio(accountNo) {
    try {
      const [historyPayload, latestPayload] = await Promise.all([
        apiGet(`/api/portfolio/history?account=${accountNo}&limit=360`),
        apiGet(`/api/portfolio/latest?account=${accountNo}`)
      ]);
      setHistoryData(historyPayload.history || []);
      setLatestPortfolio(latestPayload);
    } catch (requestError) {
      if (requestError.status === 401) {
        clearToken();
        setAuthenticated(false);
      }
      setError(requestError.message);
    }
  }

  async function loadPriceSeries(code) {
    try {
      const payload = await apiGet(`/api/etf/price-series?code=${code}&days=360`);
      setPriceSeries(payload);
    } catch (requestError) {
      setError(requestError.message);
    }
  }

  async function onSubmitLogin(event) {
    event.preventDefault();
    setSubmitting(true);
    setLoginError('');

    try {
      const payload = await apiPost('/api/auth/login', loginForm);
      if (payload.token) {
        setToken(payload.token);
      }
      setAuthenticated(true);
      setLoginRequired(Boolean(payload.login_required));
    } catch (requestError) {
      setLoginError(requestError.message);
      setAuthenticated(false);
    } finally {
      setSubmitting(false);
    }
  }

  async function onLogout() {
    try {
      await apiPost('/api/auth/logout');
    } catch (requestError) {
      // logout is best-effort
    }
    clearToken();
    setAuthenticated(false);
    setSummary(null);
    setHistoryData([]);
    setLatestPortfolio(null);
    setPriceSeries({ code: '', name: '', series: [] });
    setError('');
  }

  const tradeStatusChartData = useMemo(() => {
    const counts = {};
    (summary?.recent_trades || [])
      .filter((trade) => account === null || account === undefined || trade.account === account)
      .forEach((trade) => {
        const status = trade.status || 'UNKNOWN';
        counts[status] = (counts[status] || 0) + 1;
      });
    return Object.entries(counts).map(([status, count]) => ({
      status,
      count
    }));
  }, [summary, account]);

  const selectedAccountSummary = useMemo(() => {
    return (summary?.accounts || []).find((item) => item.account === account) || null;
  }, [summary, account]);

  const accountTrades = useMemo(() => {
    if (account === null || account === undefined) return summary?.recent_trades || [];
    return (summary?.recent_trades || []).filter((trade) => trade.account === account);
  }, [summary, account]);

  const strategySelection = useMemo(() => summary?.strategy_selection || { plan_date: null, items: [] }, [summary]);

  const strategyRiskItems = useMemo(
    () => (strategySelection.items || []).filter((item) => item.sleeve === 'RISK'),
    [strategySelection]
  );

  const strategyBondItems = useMemo(
    () => (strategySelection.items || []).filter((item) => item.sleeve === 'BOND'),
    [strategySelection]
  );

  const bondBuyFailuresForAccount = useMemo(() => {
    const rows = summary?.bond_buy_failures || [];
    if (account === null || account === undefined) return rows;
    return rows.filter((row) => row.account === account);
  }, [summary, account]);

  const bondBuyFailureReasonRows = useMemo(() => {
    const map = {};
    bondBuyFailuresForAccount.forEach((row) => {
      const reason = (row.message || '').trim() || '사유 미상';
      map[reason] = (map[reason] || 0) + 1;
    });
    return Object.entries(map)
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count);
  }, [bondBuyFailuresForAccount]);

  const filtered70 = useMemo(() => {
    const rows = etfData.limit_70 || [];
    const keyword = searchWord.trim().toLowerCase();
    if (!keyword) return rows;
    return rows.filter((item) => item.code.toLowerCase().includes(keyword) || item.name.toLowerCase().includes(keyword));
  }, [etfData.limit_70, searchWord]);

  const filtered100 = useMemo(() => {
    const rows = etfData.limit_100 || [];
    const keyword = searchWord.trim().toLowerCase();
    if (!keyword) return rows;
    return rows.filter((item) => item.code.toLowerCase().includes(keyword) || item.name.toLowerCase().includes(keyword));
  }, [etfData.limit_100, searchWord]);

  const sleeveTargets = useMemo(() => {
    const risk = Number(summary?.sleeve_targets?.risk_weight ?? 0.7);
    const bond = Number(summary?.sleeve_targets?.bond_weight ?? 0.25);
    const cash = Number(summary?.sleeve_targets?.cash_buffer_weight ?? Math.max(0, 1 - risk - bond));
    return { risk, bond, cash };
  }, [summary]);

  const mobileTabs = [
    { key: 'overview', label: '개요' },
    { key: 'portfolio', label: '계좌' },
    { key: 'etf', label: 'ETF' },
    { key: 'diagnostics', label: '진단' }
  ];

  if (!authReady) {
    return <div className="app-shell"><div className="loading-box">인증 상태 확인 중...</div></div>;
  }

  if (loginRequired && !authenticated) {
    return (
      <div className="app-shell login-shell">
        <section className="login-panel animate-in">
          <p className="eyebrow">site2 secure access</p>
          <h1>로그인</h1>
          <p className="sub">저장된 계정으로 인증 후 대시보드에 접근할 수 있습니다.</p>

          <form className="login-form" onSubmit={onSubmitLogin}>
            <label htmlFor="username">아이디</label>
            <input
              id="username"
              autoComplete="username"
              value={loginForm.username}
              onChange={(event) => setLoginForm((prev) => ({ ...prev, username: event.target.value }))}
              required
            />

            <label htmlFor="password">비밀번호</label>
            <input
              id="password"
              type="password"
              autoComplete="current-password"
              value={loginForm.password}
              onChange={(event) => setLoginForm((prev) => ({ ...prev, password: event.target.value }))}
              required
            />

            {loginError ? <p className="error-text">{loginError}</p> : null}
            <button type="submit" disabled={submitting}>{submitting ? '로그인 중...' : '로그인'}</button>
          </form>
        </section>
      </div>
    );
  }

  return (
    <div className={`app-shell mobile-tab-${mobileTab}`}>
      <header className="hero animate-in">
        <div>
          <p className="eyebrow">site2 react dashboard</p>
          <h1>ETF 리밸런싱 대시보드</h1>
          <p className="sub">
            `site2.db` + `etf_daily_close_selected_universe.db` 기반 실시간 모니터링
          </p>
        </div>
        <div className="hero-actions">
          <div className="asof">기준시각: {summary?.generated_at ? formatTimeLabel(summary.generated_at) : '-'}</div>
          {loginRequired ? <button className="ghost" onClick={onLogout}>로그아웃</button> : null}
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}
      {loading ? <div className="loading-box">데이터 로딩 중...</div> : null}

      <section className="mobile-top-tabs">
        {mobileTabs.map((tab) => (
          <button
            key={tab.key}
            className={mobileTab === tab.key ? 'active' : ''}
            onClick={() => setMobileTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </section>

      <div className={`mobile-pane ${mobileTab === 'overview' ? 'active' : ''}`}>
        <section className="account-tiles animate-in delay-1">
          {(summary?.accounts || []).map((item) => (
            <button
              key={item.account}
              className={`account-tile ${item.account === account ? 'active' : ''}`}
              onClick={() => setAccount(item.account)}
            >
              <div className="account-tile-head">계좌 {item.account}</div>
              <div className="account-tile-value">{formatKrw(item.total_krw)}</div>
              <div className="account-tile-meta">
                투자 {formatKrw(item.invested_krw)} · 현금 {formatKrw(item.cash_krw)}
              </div>
            </button>
          ))}
        </section>

        <section className="stats-grid">
          <article className="card stat animate-in delay-1">
            <h3>계좌 {account ?? '-'} 총 자산</h3>
            <div className="value">{formatKrw(selectedAccountSummary?.total_krw || 0)}</div>
            <p>선택 계좌 기준</p>
          </article>
          <article className="card stat animate-in delay-2">
            <h3>계좌 {account ?? '-'} 투자 금액</h3>
            <div className="value">{formatKrw(selectedAccountSummary?.invested_krw || 0)}</div>
            <p>현금 제외 평가액</p>
          </article>
          <article className="card stat animate-in delay-3">
            <h3>계좌 {account ?? '-'} 현금</h3>
            <div className="value">{formatKrw(selectedAccountSummary?.cash_krw || 0)}</div>
            <p>가용 현금</p>
          </article>
          <article className="card stat animate-in delay-4">
            <h3>계좌 {account ?? '-'} 보유 종목수</h3>
            <div className="value">{numberFormat.format(selectedAccountSummary?.holding_count || 0)}개</div>
            <p>최근 스냅샷 기준</p>
          </article>
        </section>

        <section className="card animate-in delay-2">
          <div className="section-head">
            <div>
              <h2>계좌별 자산 추이</h2>
              <p>선택 계좌 {account ?? '-'} 스냅샷 히스토리</p>
            </div>
            <div className="mini-total">합산 총자산 {formatKrw(summary?.portfolio_totals?.total_krw || 0)}</div>
          </div>

          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height={290}>
              <AreaChart data={historyData} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
                <defs>
                  <linearGradient id="equityFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#0e9a6c" stopOpacity={0.45} />
                    <stop offset="100%" stopColor="#0e9a6c" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#d9e3da" />
                <XAxis dataKey="captured_at" tickFormatter={formatTimeLabel} minTickGap={28} />
                <YAxis tickFormatter={(value) => `${Math.round(value / 10000)}만`} />
                <Tooltip
                  formatter={(value) => formatKrw(value)}
                  labelFormatter={(label) => `시각 ${label}`}
                />
                <Legend />
                <Area type="monotone" dataKey="total_krw" stroke="#0e9a6c" fill="url(#equityFill)" name="총 자산" strokeWidth={2.4} />
                <Area type="monotone" dataKey="cash_krw" stroke="#d86b2b" fillOpacity={0} name="현금" strokeWidth={1.8} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </section>
      </div>

      <div className={`mobile-pane ${mobileTab === 'portfolio' ? 'active' : ''}`}>
        <section className="two-col">
          <article className="card animate-in delay-3">
            <div className="section-head">
              <div>
                <h2>계좌 {account ?? '-'} 현재 보유 종목</h2>
                <p>{latestPortfolio?.captured_at || '-'}</p>
              </div>
              <div className="mini-total">
                <span>총자산 {formatKrw(latestPortfolio?.total_krw || 0)}</span>
              </div>
            </div>

            <div className="table-wrap compact">
              <table>
                <thead>
                  <tr>
                    <th>코드</th>
                    <th>종목</th>
                    <th>수량</th>
                    <th>평가금액</th>
                    <th>비중</th>
                    <th>한도</th>
                  </tr>
                </thead>
                <tbody>
                  {(latestPortfolio?.holdings || []).length === 0 ? (
                    <tr>
                      <td colSpan="6">보유 종목 데이터가 없습니다.</td>
                    </tr>
                  ) : (
                    (latestPortfolio?.holdings || []).map((item) => (
                      <tr
                        key={item.code}
                        className={item.code === selectedCode ? 'selected' : ''}
                        onClick={() => openChartForCode(item.code)}
                      >
                        <td>{item.code}</td>
                        <td>{item.name}</td>
                        <td>{numberFormat.format(item.qty)}</td>
                        <td>{formatKrw(item.eval_amount)}</td>
                        <td>{formatPct(item.weight)}</td>
                        <td>
                          <span className={`limit-chip ${item.allocation_limit_pct === 100 ? 'safe' : 'risk'}`}>
                            {item.allocation_limit_pct}%
                          </span>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="card animate-in delay-4">
            <div className="section-head">
              <div>
                <h2>계좌 {account ?? '-'} 주문 상태 분포</h2>
                <p>최근 주문 20건 중 선택 계좌 기준</p>
              </div>
            </div>
            <div className="chart-wrap donut">
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={tradeStatusChartData}
                    dataKey="count"
                    nameKey="status"
                    innerRadius={62}
                    outerRadius={95}
                    paddingAngle={3}
                  >
                    {tradeStatusChartData.map((entry, index) => (
                      <Cell key={`${entry.status}-${index}`} fill={index % 2 === 0 ? '#0e9a6c' : '#d86b2b'} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => `${value}건`} />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="status-list">
              {(tradeStatusChartData || []).length === 0 ? (
                <div className="status-row">주문 데이터가 없습니다.</div>
              ) : (
                (tradeStatusChartData || []).map((item) => (
                  <div key={item.status} className="status-row">
                    <StatusBadge value={item.status} />
                    <strong>{numberFormat.format(item.count)}건</strong>
                  </div>
                ))
              )}
            </div>
          </article>
        </section>

        <section className="card animate-in delay-4">
          <div className="section-head">
            <div>
              <h2>계좌 {account ?? '-'} 최근 주문 내역</h2>
              <p>선택 계좌 기준 최근 20건</p>
            </div>
          </div>
          <div className="table-wrap compact">
            <table>
              <thead>
                <tr>
                  <th>요청시각</th>
                  <th>계좌</th>
                  <th>코드</th>
                  <th>매매</th>
                  <th>수량</th>
                  <th>가격</th>
                  <th>상태</th>
                </tr>
              </thead>
              <tbody>
                {accountTrades.length === 0 ? (
                  <tr>
                    <td colSpan="7">선택 계좌 주문 내역이 없습니다.</td>
                  </tr>
                ) : (
                  accountTrades.map((trade, idx) => (
                    <tr key={`${trade.requested_at}-${trade.code}-${idx}`}>
                      <td>{formatTimeLabel(trade.requested_at)}</td>
                      <td>{trade.account}</td>
                      <td>{trade.code}</td>
                      <td>{trade.side}</td>
                      <td>{numberFormat.format(trade.qty)}</td>
                      <td>{formatKrw(trade.price)}</td>
                      <td><StatusBadge value={trade.status} /></td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <div className={`mobile-pane ${mobileTab === 'etf' ? 'active' : ''}`}>
        <section className="card animate-in delay-2">
          <div className="section-head">
            <div>
              <h2>전략 선정 종목</h2>
              <p>
                기준일 {strategySelection.plan_date || '-'} · RISK {formatPct(sleeveTargets.risk, 1)} + BOND {formatPct(sleeveTargets.bond, 1)} + CASH {formatPct(sleeveTargets.cash, 1)}
              </p>
            </div>
          </div>
          <div className="etf-grid">
            <div className="table-panel">
              <h3>RISK 선정</h3>
              <div className="table-wrap compact">
                <table>
                  <thead>
                    <tr>
                      <th>코드</th>
                      <th>종목</th>
                      <th>목표비중</th>
                      <th>차트</th>
                    </tr>
                  </thead>
                  <tbody>
                    {strategyRiskItems.length === 0 ? (
                      <tr><td colSpan="4">선정 데이터가 없습니다.</td></tr>
                    ) : (
                      strategyRiskItems.map((item) => (
                        <tr key={`risk-${item.code}`}>
                          <td>{item.code}</td>
                          <td>{item.name || '-'}</td>
                          <td>{formatPct(item.weight)}</td>
                          <td><button className="mini-btn" onClick={() => openChartForCode(item.code)}>보기</button></td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="table-panel">
              <h3>BOND 선정</h3>
              <div className="table-wrap compact">
                <table>
                  <thead>
                    <tr>
                      <th>코드</th>
                      <th>종목</th>
                      <th>목표비중</th>
                      <th>차트</th>
                    </tr>
                  </thead>
                  <tbody>
                    {strategyBondItems.length === 0 ? (
                      <tr><td colSpan="4">선정 데이터가 없습니다.</td></tr>
                    ) : (
                      strategyBondItems.map((item) => (
                        <tr key={`bond-${item.code}`}>
                          <td>{item.code}</td>
                          <td>{item.name || '-'}</td>
                          <td>{formatPct(item.weight)}</td>
                          <td><button className="mini-btn" onClick={() => openChartForCode(item.code)}>보기</button></td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </section>

        <section className="card animate-in delay-3">
          <div className="section-head">
            <div>
              <h2>ETF 목록 (70% / 100% 구분)</h2>
              <p>
                기준일 {etfData.as_of_trade_date || '-'} · 70% {numberFormat.format(etfData.counts?.limit_70_count || 0)}개 · 100% {numberFormat.format(etfData.counts?.limit_100_count || 0)}개
              </p>
            </div>
            <input
              className="search-input"
              placeholder="코드/종목 검색"
              value={searchWord}
              onChange={(event) => setSearchWord(event.target.value)}
            />
          </div>

          <div className="etf-grid">
            <div className="table-panel">
              <h3>70% 가능 ETF (RISK)</h3>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>코드</th>
                      <th>종목</th>
                      <th>3년수익률</th>
                      <th>종가</th>
                      <th>보유중</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered70.map((item) => (
                      <tr key={item.code} className={item.code === selectedCode ? 'selected' : ''} onClick={() => openChartForCode(item.code)}>
                        <td>{item.code}</td>
                        <td>{item.name}</td>
                        <td>{formatPct(item.total_return_3y)}</td>
                        <td>{formatKrw(item.latest_close)}</td>
                        <td>{item.currently_held ? 'Y' : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="table-panel">
              <h3>100% 가능 ETF (BOND)</h3>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>코드</th>
                      <th>종목</th>
                      <th>3년수익률</th>
                      <th>종가</th>
                      <th>보유중</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered100.map((item) => (
                      <tr key={item.code} className={item.code === selectedCode ? 'selected' : ''} onClick={() => openChartForCode(item.code)}>
                        <td>{item.code}</td>
                        <td>{item.name}</td>
                        <td>{formatPct(item.total_return_3y)}</td>
                        <td>{formatKrw(item.latest_close)}</td>
                        <td>{item.currently_held ? 'Y' : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </section>
      </div>

      <div className={`mobile-pane ${mobileTab === 'diagnostics' ? 'active' : ''}`}>
        <section className="card animate-in delay-3">
          <div className="section-head">
            <div>
              <h2>채권 ETF 매수 실패 원인</h2>
              <p>계좌 {account ?? '-'} 기준 · 최근 실패 주문 로그</p>
            </div>
          </div>
          <div className="failure-grid">
            <div className="failure-summary">
              {bondBuyFailureReasonRows.length === 0 ? (
                <div className="status-row">최근 채권 ETF 매수 실패가 없습니다.</div>
              ) : (
                bondBuyFailureReasonRows.map((row) => (
                  <div key={row.reason} className="status-row">
                    <span>{row.reason}</span>
                    <strong>{row.count}건</strong>
                  </div>
                ))
              )}
            </div>
            <div className="table-wrap compact">
              <table>
                <thead>
                  <tr>
                    <th>시각</th>
                    <th>코드</th>
                    <th>종목</th>
                    <th>사유</th>
                  </tr>
                </thead>
                <tbody>
                  {bondBuyFailuresForAccount.length === 0 ? (
                    <tr><td colSpan="4">실패 내역이 없습니다.</td></tr>
                  ) : (
                    bondBuyFailuresForAccount.slice(0, 20).map((row, idx) => (
                      <tr key={`${row.requested_at}-${row.code}-${idx}`}>
                        <td>{formatTimeLabel(row.requested_at)}</td>
                        <td>{row.code}</td>
                        <td>{row.name || '-'}</td>
                        <td>{row.message || '-'}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </div>

      <nav className="mobile-bottom-tabs">
        {mobileTabs.map((tab) => (
          <button
            key={tab.key}
            className={mobileTab === tab.key ? 'active' : ''}
            onClick={() => setMobileTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {chartModalOpen ? (
        <div className="modal-backdrop" onClick={() => setChartModalOpen(false)}>
          <div className="modal-panel" onClick={(event) => event.stopPropagation()}>
            <div className="modal-head">
              <div>
                <h3>ETF 차트</h3>
                <p>{priceSeries.code ? `${priceSeries.code} ${priceSeries.name}` : '로딩 중...'}</p>
              </div>
              <button className="ghost dark" onClick={() => setChartModalOpen(false)}>닫기</button>
            </div>
            <div className="chart-wrap">
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={priceSeries.series || []} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9e3da" />
                  <XAxis dataKey="trade_date" tickFormatter={formatShortDate} minTickGap={25} />
                  <YAxis yAxisId="left" tickFormatter={(value) => numberFormat.format(Math.round(value))} />
                  <YAxis yAxisId="right" orientation="right" tickFormatter={(value) => `${(value * 100).toFixed(0)}%`} />
                  <Tooltip
                    formatter={(value, name) => {
                      if (name === 'close_price') return [formatKrw(value), '종가'];
                      return [formatPct(value), '기간 수익률'];
                    }}
                  />
                  <Legend />
                  <Line yAxisId="left" type="monotone" dataKey="close_price" stroke="#0f3b4a" strokeWidth={2.2} dot={false} name="종가" />
                  <Line yAxisId="right" type="monotone" dataKey="change_pct" stroke="#d86b2b" strokeWidth={2} dot={false} name="기간 수익률" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default App;
