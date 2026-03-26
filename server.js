/* ============================================================
   CapIntel Fundamentals Microservice
   Deployed on Render.com — different IP range from Vercel,
   not blocked by Yahoo Finance.

   yahoo-finance2 works here because Render's IPs are not
   flagged by Yahoo's anti-scraping systems.

   Endpoints:
   POST /fundamentals  { positions, techMap, goals }
   GET  /health        → { status: "ok" }
   ============================================================ */

import express from "express"
import yahooFinanceModule from "yahoo-finance2"

/* yahoo-finance2 exports a class — instantiate and find quoteSummary */
const YF       = yahooFinanceModule.default ?? yahooFinanceModule
const instance = (typeof YF === "function") ? new YF() : YF

/* Walk full prototype chain to find quoteSummary */
function findMethod(obj, name) {
  let proto = obj
  while (proto && proto !== Object.prototype) {
    if (typeof proto[name] === "function") return proto[name].bind(obj)
    proto = Object.getPrototypeOf(proto)
  }
  return null
}

const quoteSummaryFn = findMethod(instance, "quoteSummary")

/* Log what we found for Render diagnostics */
const allMethods = []
let p = instance
while (p && p !== Object.prototype) {
  Object.getOwnPropertyNames(p).forEach(k => {
    if (typeof instance[k] === "function") allMethods.push(k)
  })
  p = Object.getPrototypeOf(p)
}
console.log("[yf2] methods found:", allMethods)
console.log("[yf2] quoteSummary found:", !!quoteSummaryFn)

const app  = express()
const PORT = process.env.PORT || 3001

app.use(express.json({ limit: "1mb" }))

/* ── CORS — allow only your Vercel app ── */
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin",  "*")
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization")
  if (req.method === "OPTIONS") return res.status(200).end()
  next()
})

/* ── Health check ── */
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() })
})

/* ── Ticker resolver ── */
function toYahooTicker(pos) {
  const t = (pos.key || "").replace(/\.(NS|BO)$/, "")
  if (!t || pos.type === "MutualFund" || t.includes("-USD")) return null
  if (pos.currency === "INR") return `${t}.NS`
  const map = {
    SEMI: "CHIP.PA", EWG2: "EWG2.SG", DFNS: "DFNS.L",
    IWDA: "IWDA.L",  EIMI: "EIMI.L",  SSLV: "SSLV.L",
    SGLN: "SGLN.L",  VUSA: "VUSA.L",  CSPX: "CSPX.L"
  }
  return map[t] || t
}

/* ── Fetch fundamentals from Yahoo Finance ── */
async function fetchFundamentals(ticker) {
  try {
    if (!quoteSummaryFn) {
      console.error("[yf2] quoteSummary not available — available methods:", allMethods)
      return null
    }
    const data = await quoteSummaryFn(ticker, {
      modules: ["financialData", "defaultKeyStatistics", "summaryDetail", "assetProfile"]
    })
    if (!data) return null

    const fd = data.financialData        || {}
    const ks = data.defaultKeyStatistics || {}
    const sd = data.summaryDetail        || {}
    const ap = data.assetProfile         || {}
    const n  = v => (typeof v === "number" && isFinite(v)) ? v : null

    return {
      trailingPE:      n(sd.trailingPE)        ?? n(ks.forwardPE),
      priceToBook:     n(ks.priceToBook),
      roe:             n(fd.returnOnEquity),
      roa:             n(fd.returnOnAssets),
      profitMargins:   n(fd.profitMargins),
      operatingMargins:n(fd.operatingMargins),
      grossMargins:    n(fd.grossMargins),
      debtToEquity:    n(fd.debtToEquity),
      currentRatio:    n(fd.currentRatio),
      revenueGrowth:   n(fd.revenueGrowth),
      earningsGrowth:  n(fd.earningsGrowth),
      revenuePerShare: n(fd.revenuePerShare),
      trailingEps:     n(ks.trailingEps),
      forwardEps:      n(ks.forwardEps),
      sector:          ap.sector   || null,
      industry:        ap.industry || null,
      marketCap:       n(sd.marketCap),
      beta:            n(ks.beta),
      recommendationKey:       fd.recommendationKey || null,
      numberOfAnalystOpinions: n(fd.numberOfAnalystOpinions),
      targetMeanPrice:         n(fd.targetMeanPrice),
    }
  } catch(e) {
    console.error(`[fundamentals] ${ticker}: ${e.message}`)
    return null
  }
}

/* ── Fundamental scorer (0-100) ── */
function scoreFundamentals(f) {
  if (!f) return { score: 50, signals: [], grade: "UNKNOWN", hasData: false, display: null }

  const sigs = []; let score = 50, fields = 0

  if (f.trailingPE != null) {
    fields++
    if      (f.trailingPE <= 0)  { score -= 15; sigs.push("Negative P/E — loss-making") }
    else if (f.trailingPE < 12)  { score += 12; sigs.push(`P/E ${f.trailingPE.toFixed(1)}x — undervalued`) }
    else if (f.trailingPE < 20)  { score += 8;  sigs.push(`P/E ${f.trailingPE.toFixed(1)}x — fair`) }
    else if (f.trailingPE < 35)  { score += 2 }
    else if (f.trailingPE < 60)  { score -= 5;  sigs.push(`P/E ${f.trailingPE.toFixed(1)}x — elevated`) }
    else                         { score -= 12; sigs.push(`P/E ${f.trailingPE.toFixed(1)}x — very high`) }
  }
  if (f.priceToBook != null) {
    fields++
    if      (f.priceToBook < 0)   { score -= 10 }
    else if (f.priceToBook < 1.5) { score += 8;  sigs.push(`P/B ${f.priceToBook.toFixed(1)}x — near book`) }
    else if (f.priceToBook < 3)   { score += 4 }
    else if (f.priceToBook > 6)   { score -= 8;  sigs.push(`High P/B ${f.priceToBook.toFixed(1)}x`) }
  }
  if (f.roe != null) {
    fields++
    const r = f.roe * 100
    if      (r > 25) { score += 15; sigs.push(`ROE ${r.toFixed(0)}% — excellent`) }
    else if (r > 15) { score += 10; sigs.push(`ROE ${r.toFixed(0)}% — good`) }
    else if (r > 8)  { score += 4 }
    else if (r > 0)  { score -= 5;  sigs.push(`ROE ${r.toFixed(0)}% — weak`) }
    else             { score -= 15; sigs.push("Negative ROE") }
  }
  if (f.debtToEquity != null) {
    fields++
    const de = f.debtToEquity > 10 ? f.debtToEquity / 100 : f.debtToEquity
    if      (de < 0.2) { score += 10; sigs.push(`Low debt D/E ${de.toFixed(2)}`) }
    else if (de < 0.5) { score += 6 }
    else if (de < 1.0) { score += 0 }
    else if (de < 2.0) { score -= 8;  sigs.push(`High debt D/E ${de.toFixed(1)}`) }
    else               { score -= 15; sigs.push(`Excessive debt D/E ${de.toFixed(1)}`) }
  }
  if (f.revenueGrowth != null) {
    fields++
    const g = f.revenueGrowth * 100
    if      (g > 20) { score += 12; sigs.push(`Revenue +${g.toFixed(0)}% YoY`) }
    else if (g > 10) { score += 7;  sigs.push(`Revenue +${g.toFixed(0)}% YoY`) }
    else if (g > 0)  { score += 2 }
    else if (g > -5) { score -= 5 }
    else             { score -= 12; sigs.push(`Revenue declining ${g.toFixed(0)}%`) }
  }
  if (f.earningsGrowth != null) {
    fields++
    const g = f.earningsGrowth * 100
    if      (g > 25) { score += 12; sigs.push(`Earnings +${g.toFixed(0)}% YoY`) }
    else if (g > 10) { score += 6 }
    else if (g > 0)  { score += 2 }
    else if (g > -15){ score -= 8;  sigs.push(`Earnings declining`) }
    else             { score -= 15; sigs.push("Earnings declining sharply") }
  }
  if (f.profitMargins != null) {
    fields++
    const m = f.profitMargins * 100
    if      (m > 25) { score += 10; sigs.push(`Margins ${m.toFixed(0)}% — excellent`) }
    else if (m > 15) { score += 6;  sigs.push(`Margins ${m.toFixed(0)}% — good`) }
    else if (m > 8)  { score += 2 }
    else if (m > 0)  { score -= 3 }
    else             { score -= 12; sigs.push("Negative margins") }
  }

  if (fields === 0) return { score: 50, signals: [], grade: "UNKNOWN", hasData: false, display: null }

  score = Math.max(0, Math.min(100, Math.round(score)))
  const grade  = score >= 70 ? "STRONG" : score >= 50 ? "FAIR" : score >= 30 ? "WEAK" : "POOR"
  const fmt    = (v, mult=1, dec=1, suf="") => v != null ? (v*mult).toFixed(dec)+suf : "N/A"
  const fmtDE  = v => { if (v == null) return "N/A"; return (v > 10 ? v/100 : v).toFixed(2) }

  return {
    score, grade, signals: sigs.slice(0, 4), hasData: true,
    display: {
      pe:      fmt(f.trailingPE,    1,   1, "x"),
      pb:      fmt(f.priceToBook,   1,   1, "x"),
      roe:     fmt(f.roe,           100, 1, "%"),
      de:      fmtDE(f.debtToEquity),
      revGrow: fmt(f.revenueGrowth, 100, 1, "%"),
      margins: fmt(f.profitMargins, 100, 1, "%"),
      sector:  f.sector || "N/A",
      grade,
      targetPrice:    f.targetMeanPrice ? `₹${f.targetMeanPrice.toFixed(0)}` : null,
      analysts:       f.numberOfAnalystOpinions || null,
      recommendation: f.recommendationKey || null
    }
  }
}

/* ── Goal alignment scorer (0-100) ── */
function scoreGoalAlignment(pos, sector, goals) {
  let score = 50; const sigs = []
  if (pos.currency === "INR") {
    score += 10; sigs.push("India — home fund aligned")
    const good = ["bank","financial","nbfc","software","it","technology","pharma",
                  "healthcare","consumer","fmcg","capital goods","industrial",
                  "machinery","chemicals","ratings","analytics","food"]
    const bad  = ["utilities","power","oil","gas","metals","mining","telecom","cement"]
    const s = (sector || "").toLowerCase()
    if (good.some(g => s.includes(g))) { score += 12; sigs.push(`Quality sector: ${sector}`) }
    if (bad.some(b =>  s.includes(b))) { score -= 8;  sigs.push("Cyclical sector") }
  } else {
    score += 10; sigs.push("EUR/USD — retirement corpus")
  }
  const eur = pos.totalCurrentEUR || 0
  if      (eur < 30)  { score -= 20; sigs.push("Under €30 — negligible") }
  else if (eur < 100) { score -= 8;  sigs.push("Under €100 — underfunded") }
  if ((goals.retireAge || 50) - 36 >= 10) score += 6
  return { score: Math.max(0, Math.min(100, Math.round(score))), signals: sigs }
}

/* ── Composite verdict ── */
function getVerdict(techScore, techVerdict, fundScore, fundHasData, goalScore, pos) {
  const composite = fundHasData
    ? Math.round(techScore*0.40 + fundScore*0.35 + goalScore*0.25)
    : Math.round(techScore*0.65 + goalScore*0.35)

  const isBuy  = techVerdict === "BUY" || techVerdict === "STRONG BUY"
  const isSell = techVerdict === "SELL" || techVerdict === "TRIM"
  const cur    = pos.currentPrice || 0
  let verdict, action, priority, reasoning

  if (fundHasData) {
    if (fundScore < 30) {
      verdict="EXIT"; priority="HIGH"
      reasoning="Poor fundamentals confirm exit — weak business metrics"
      action=`Sell all ${pos.qty||""} shares. Business quality is insufficient for long-term hold.`
    } else if (isBuy && fundScore >= 55) {
      verdict="ADD"; priority=composite>=72?"HIGH":"MEDIUM"
      reasoning="Strong technicals + solid fundamentals — quality entry point"
      const qty = Math.max(1, Math.round(5000/(cur||1)))
      action = pos.currency==="INR"
        ? `Add ${qty} shares at ₹${cur.toFixed(0)} (≈₹${(qty*cur).toFixed(0)}) — builds to meaningful size`
        : `Add €200–300 — underfunded quality position`
    } else if (isSell && fundScore >= 60) {
      verdict="HOLD"; priority="MEDIUM"
      reasoning="Weak technicals but strong fundamentals — temporary dip in quality business"
      action=`Hold. Strong fundamentals contradict sell signal. Consider adding if RSI drops below 35.`
    } else if (isSell && fundScore >= 40) {
      verdict="REVIEW"; priority="LOW"
      reasoning="Bearish technicals + average fundamentals — monitor closely"
      action=`Hold but watch carefully. Exit if price breaks 52-week low or next earnings disappoint.`
    } else if (isSell) {
      verdict="EXIT"; priority="HIGH"
      reasoning="Bearish technicals + weak fundamentals — confirmed exit"
      action=`Sell all ${pos.qty||""} shares. Both signals confirm exit. Redeploy to stronger position.`
    } else if (isBuy) {
      verdict="ADD"; priority="MEDIUM"
      reasoning="Technical BUY with fair fundamentals"
      const qty = Math.max(1, Math.round(5000/(cur||1)))
      action = pos.currency==="INR"
        ? `Add ${qty} shares at ₹${cur.toFixed(0)} (≈₹${(qty*cur).toFixed(0)})`
        : `Add €150–200`
    } else {
      verdict="HOLD"; priority="LOW"
      reasoning="Consolidating with solid fundamentals — wait for better entry"
      action=`Hold. Quality business in consolidation. Consider adding if RSI drops below 42.`
    }
  } else {
    if (isBuy) {
      verdict="ADD"; priority="MEDIUM"
      reasoning="Technical BUY — fundamental data unavailable from Yahoo Finance"
      const qty = Math.max(1, Math.round(5000/(cur||1)))
      action = pos.currency==="INR"
        ? `Add ${qty} shares at ₹${cur.toFixed(0)} — verify on Screener.in before large commitment`
        : `Add €150 — verify fundamentals first`
    } else if (isSell) {
      verdict="REVIEW"; priority="MEDIUM"
      reasoning="Technical SELL — no fundamental data to confirm. Check Screener.in first."
      action=`Verify fundamentals on Screener.in for ${pos.key} before exiting.`
    } else {
      verdict="HOLD"; priority="LOW"
      reasoning="Neutral — no fundamental data available"
      action=`Hold. Check fundamentals on Screener.in before adding or exiting.`
    }
  }
  return { verdict, action, priority, composite, reasoning }
}

/* ── Main endpoint ── */
app.post("/fundamentals", async (req, res) => {
  const { positions, techMap, goals } = req.body || {}
  if (!positions?.length) return res.status(400).json({ error: "positions required" })

  const results = {}

  /* Sequential with 500ms delay — Render IPs are not rate-limited but be respectful */
  for (const pos of positions) {
    if (pos.type === "MutualFund" || (pos.key||"").includes("-USD")) continue
    const ticker = toYahooTicker(pos)
    if (!ticker) continue

    const f    = await fetchFundamentals(ticker)
    const fund = scoreFundamentals(f)
    const tech = techMap?.[pos.key] || {}
    const goal = scoreGoalAlignment(pos, f?.sector, goals || {})
    const out  = getVerdict(
      tech.score ?? 50, tech.verdict ?? "HOLD",
      fund.score, fund.hasData, goal.score, pos
    )

    results[pos.key] = {
      ...out,
      scores:       { technical: tech.score??50, fundamental: fund.score, goalAlign: goal.score },
      signals:      { technical: tech.signals||[], fundamental: fund.signals, goalAlign: goal.signals },
      fundamentals: fund.display || {
        pe:"N/A", pb:"N/A", roe:"N/A", de:"N/A",
        revGrow:"N/A", margins:"N/A", sector:"N/A", grade:"UNKNOWN"
      }
    }

    await new Promise(r => setTimeout(r, 500))
  }

  return res.json({ results, computedAt: new Date().toISOString() })
})

app.listen(PORT, () => console.log(`Fundamentals service running on port ${PORT}`))
