def Golden_Filter_TURBO(filename):
    print(f'\n🛡️ FINAL SYSTEM CHECK: Starting Golden Filter TURBO for: {filename}')

    import pandas as pd
    import yfinance as yf
    from datetime import datetime
    import os
    import warnings
    warnings.filterwarnings('ignore')

    file_tag = 'GENERAL'
    filename_upper = filename.upper()
    if 'ETF' in filename_upper:
        file_tag = 'ETF'
    elif 'STOCK' in filename_upper or 'STK' in filename_upper:
        file_tag = 'STOCKS'

    print(f'📂 DETECTED MODE: {file_tag}')
    print('-' * 50)

    try:
        if filename.endswith('.xlsx'):
            df = pd.read_excel(filename)
        else:
            df = pd.read_csv(filename)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not read file '{filename}'. Details: {e}")
        return

    df.columns = df.columns.str.strip()

    if 'Reason' in df.columns:
        df = df.rename(columns={'Reason': 'Orig_Reason'})
    else:
        df['Orig_Reason'] = ''

    col_map = {
        'Deep_Score': 'Score',
        'Entry': 'Entry',
        '🔵 ENTRY': 'Entry',
        'Ticker': 'Ticker',
        'Verdict': 'Verdict',
        'Real_Size_AUM': 'Size'
    }
    df = df.rename(columns=col_map)

    if 'Ticker' not in df.columns:
        print('❌ CRITICAL ERROR: No Ticker column found.')
        return

    df['Ticker'] = df['Ticker'].astype(str).str.strip().str.upper()
    df = df.drop_duplicates(subset='Ticker', keep='first')

    if 'Verdict' in df.columns:
        mask = df['Verdict'].astype(str).str.contains('OK|HUGE', case=False, na=False)
        df = df[mask].copy()

    if df.empty:
        print('❌ ERROR: No assets left to scan after safety filters.')
        return

    def clean_num(x):
        try:
            return float(str(x).replace('$', '').replace(',', '').replace('%', '').strip())
        except:
            return 0.0

    def calc_recent_rs_2d(ticker):
        try:
            hist_stock = yf.Ticker(ticker).history(period='2mo')
            hist_spy = yf.Ticker('SPY').history(period='2mo')
            if hist_stock.empty or hist_spy.empty or len(hist_stock) < 3 or len(hist_spy) < 3:
                return 0.0
            stock_now = float(hist_stock['Close'].iloc[-1])
            stock_prev2 = float(hist_stock['Close'].iloc[-3])
            spy_now = float(hist_spy['Close'].iloc[-1])
            spy_prev2 = float(hist_spy['Close'].iloc[-3])
            if stock_prev2 <= 0 or spy_prev2 <= 0:
                return 0.0
            stock_ret_2d = (stock_now / stock_prev2) - 1
            spy_ret_2d = (spy_now / spy_prev2) - 1
            return (stock_ret_2d - spy_ret_2d) * 100.0
        except:
            return 0.0

    df['Score'] = df['Score'].apply(clean_num) if 'Score' in df.columns else 0.0
    df['Entry'] = df['Entry'].apply(clean_num) if 'Entry' in df.columns else 0.0

    tickers = df['Ticker'].unique().tolist()
    results = []

    print(f'📡 Scanning Market Data for {len(tickers)} assets (Optimized 2-Month Batch)...')
    print('-' * 60)

    for i, ticker in enumerate(tickers):
        try:
            print(f'⚙️ Processing [{i+1}/{len(tickers)}] {ticker}...', end=' ')
            t = yf.Ticker(ticker)
            hist = t.history(period='2mo')
            if hist.empty or len(hist) < 21:
                print(f'Skipping {ticker} - insufficient history ({len(hist)} bars)')
                continue

            current_price = float(hist['Close'].iloc[-1])
            sma20 = float(hist['Close'].iloc[-21:-1].mean())

            vol_series = hist['Volume'].copy()
            today_str = datetime.now().strftime('%Y-%m-%d')
            today_vol = float(vol_series.iloc[-1])
            is_partial_day = str(vol_series.index[-1].date()) == today_str
            clean_vol = vol_series.iloc[:-1] if is_partial_day else vol_series
            has_vol_spike = False
            if len(clean_vol) >= 5:
                avg_vol = float(clean_vol.mean())
                vol_to_check = today_vol if is_partial_day else float(clean_vol.iloc[-1])
                if avg_vol > 0 and vol_to_check > avg_vol * 1.20:
                    has_vol_spike = True

            try:
                qtype = t.info.get('quoteType', 'EQUITY')
                if qtype not in ['ETF', 'EQUITY']:
                    qtype = 'EQUITY'
            except Exception:
                qtype = 'ETF' if file_tag == 'ETF' else 'EQUITY'

            row = df[df['Ticker'] == ticker].iloc[0]
            score = float(row.get('Score', 0))
            entry_price = float(row.get('Entry', 0))

            bonus_prefix = ''
            if file_tag == 'STOCKS':
                vol_col = next((c for c in df.columns if 'VOL' in c.upper() and c != 'Volume'), None)
                profile_col = next((c for c in df.columns if 'PROFILE' in c.upper()), None)
                bonus = 0
                if vol_col:
                    try:
                        vval = str(row[vol_col]).replace('$', '').replace(',', '').strip()
                        if float(vval) > 1000000:
                            bonus = 15
                        elif float(vval) > 500000:
                            bonus = 8
                    except:
                        pass
                elif profile_col:
                    prof_str = str(row[profile_col]).upper()
                    if 'HIGH LIQ' in prof_str:
                        bonus = 15
                    elif 'MED LIQ' in prof_str:
                        bonus = 8
                if bonus > 0 and score < 94.0:
                    score = min(score + bonus, 94.0)
                    bonus_prefix = f'(+{bonus} liquidity bonus) '

            allocation = str(row.get('Allocation', 'PORTFOLIO')).strip().upper()
            pattern = str(row.get('Pattern', '')).strip().upper()
            orig_reason = str(row.get('Orig_Reason', '')).strip().upper()

            if entry_price <= 0:
                entry_price = current_price
                print(f'{ticker} No entry price found, using current price as fallback.')

            action, reason = 'WAIT', 'Waiting...'
            stop_loss_pct = 0.06
            dist_sma = ((current_price - sma20) / sma20 * 100) if sma20 > 0 else 0

            is_elite = False
            if qtype == 'ETF':
                if score >= 90:
                    action, reason = 'BUY ELITE', 'Elite ETF Safe'
                else:
                    action, reason = 'BUY NOW', 'Standard ETF'
            else:
                if score >= 90:
                    is_elite = True
                    stop_loss_pct = 0.04
                    if dist_sma > 10:
                        action, reason = 'CAUTION', bonus_prefix + f'Overheated: {dist_sma:.1f}% above SMA20'
                    elif dist_sma < -8:
                        action, reason = 'DOWNTREND', bonus_prefix + f'Below SMA20: {dist_sma:.1f}%'
                    else:
                        action, reason = 'BUY ELITE', bonus_prefix + 'Elite Stock Setup'
                else:
                    if dist_sma > 15:
                        action, reason = 'WATCH', bonus_prefix + 'Extended Run'
                    elif dist_sma < -8:
                        action, reason = 'DOWNTREND', bonus_prefix + f'Below SMA20: {dist_sma:.1f}%'
                    else:
                        action, reason = 'BUY NOW', bonus_prefix + 'Growth Setup'

            if current_price > entry_price * 1.02:
                if is_elite:
                    action, reason = 'WATCH ELITE CHASE', bonus_prefix + 'Elite ran >2%. Wait for pullback'
                else:
                    action, reason = 'CHASE', bonus_prefix + 'Price ran away >2%'
            elif current_price < entry_price * 0.98:
                action, reason = 'WAIT', bonus_prefix + 'Below Breakout Level'

            stop_price = entry_price * (1 - stop_loss_pct)
            recent_rs_2d = calc_recent_rs_2d(ticker) if file_tag == 'STOCKS' and allocation == 'PORTFOLIO' else 0.0

            results.append({
                'Ticker': ticker,
                'Type': qtype,
                'Allocation': allocation,
                'Pattern': pattern,
                'Orig_Reason': orig_reason,
                'Score': round(score, 2),
                'RecentRS2D': round(recent_rs_2d, 2),
                'AnchorScore': round(score + (recent_rs_2d * 0.15), 2),
                'EntryTrigger': round(entry_price, 2),
                'Current': round(current_price, 2),
                'VolSpike': has_vol_spike,
                'ACTION': action,
                'STOPLOSS': round(stop_price, 2),
                'Reason': reason
            })
            print('OK')
        except Exception as e:
            print(f'Warning: Failed processing {ticker} - {str(e)[:80]}')
            continue

    if not results:
        print('❌ ERROR: No results generated. Check ticker validity and network connection.')
        return

    resdf = pd.DataFrame(results)
    priority = {
        'BUY ELITE': 0,
        'BUY NOW': 1,
        'BUY Safe': 1,
        'WATCH ELITE CHASE': 2,
        'WATCH': 3,
        'CAUTION': 4,
        'DOWNTREND': 4,
        'WAIT': 5,
        'CHASE': 6
    }
    resdf['Sort'] = resdf['ACTION'].map(priority).fillna(99)
    resdf = resdf.sort_values(by=['Sort', 'Score'], ascending=[True, False]).reset_index(drop=True)
    resdf['FinalSelection'] = ''

    if file_tag == 'STOCKS':
        print('-' * 30)
        print('Core / Explore - 5 Picks')
        portfoliostocks = resdf[resdf['Allocation'].str.contains('PORTFOLIO', na=False)].copy()
        if portfoliostocks.empty:
            portfoliostocks = resdf.copy()

        core3 = []
        anchor_pool = portfoliostocks.sort_values(by=['Sort', 'AnchorScore', 'Score'], ascending=[True, False, False])
        for _, r in anchor_pool.iterrows():
            if r['Sort'] == 1:
                core3.append(r['Ticker'])
            if len(core3) == 3:
                break

        if len(core3) < 3:
            for _, r in anchor_pool.iterrows():
                if r['Ticker'] not in core3:
                    core3.append(r['Ticker'])
                if len(core3) == 3:
                    break

        explore2 = []
        for _, r in portfoliostocks.iterrows():
            if r['Ticker'] in core3 or r['Sort'] < 3:
                continue
            scorer = r['Score']
            patternr = str(r['Pattern']).upper()
            reasonr = str(r['Orig_Reason']).upper()
            tickerr = r['Ticker']
            if 70 <= scorer <= 89 and ('BREAKOUT' in patternr or 'FLAG' in patternr or 'GROWTH' in reasonr):
                if r['VolSpike']:
                    explore2.append(tickerr)
                    print(f'⚡ Explore candidate: {tickerr}')
                if len(explore2) == 2:
                    break

        if len(explore2) < 2:
            for _, r in portfoliostocks.iterrows():
                if r['Ticker'] not in core3 and r['Ticker'] not in explore2 and r['Sort'] == 3:
                    explore2.append(r['Ticker'])
                if len(explore2) == 2:
                    break

        resdf.loc[resdf['Ticker'].isin(core3), 'FinalSelection'] = 'Anchor Top 3'
        resdf.loc[resdf['Ticker'].isin(explore2), 'FinalSelection'] = 'Turbo Explore'
        print(f'🏆 Top 3: {core3}')
        print(f'🚀 Explore 2: {explore2}')

    elif file_tag == 'ETF':
        top5etf = resdf['Ticker'].head(5).tolist()
        resdf.loc[resdf['Ticker'].isin(top5etf), 'FinalSelection'] = 'Top 5 ETF'

    print('=' * 85)
    print(f"GOLDEN FILTER OVERVIEW [{file_tag}] {datetime.now().strftime('%H:%M')}")
    print('=' * 85)

    cols_to_show = ['Ticker', 'Type', 'Score', 'RecentRS2D', 'AnchorScore', 'ACTION', 'STOPLOSS', 'FinalSelection', 'Reason']
    cols_to_show = [c for c in cols_to_show if c in resdf.columns]
    print(resdf[cols_to_show].head(15).to_string(index=False))

    targetfolder = '/content/drive/MyDrive/STOCK_TRADER/WTC_SYSTEM/3_Golden_Plan'
    os.makedirs(targetfolder, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    outfilename = f'GoldenPlan_{file_tag}_{timestamp}.csv'
    outfilepath = os.path.join(targetfolder, outfilename)
    resdf.to_csv(outfilepath, index=False)
    print(f'✅ Saved Golden Plan directly to 3_Golden_Plan: {outfilename}')

    return resdf
