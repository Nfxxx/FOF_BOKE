# -*- coding: utf-8 -*-

from __future__ import division

from functools import wraps

import numpy as np
from pandas import DataFrame, Series
import pandas as pd


def series_indicator(col):
    def inner_series_indicator(f):
        @wraps(f)
        def wrapper(s, *args, **kwargs):
            if isinstance(s, DataFrame):
                s = s[col]
            return f(s, *args, **kwargs)
        return wrapper
    return inner_series_indicator


def _wilder_sum(s, n):
    s = s.dropna()

    nf = (n - 1) / n
    ws = [np.nan]*(n - 1) + [s[n - 1] + nf*sum(s[:n - 1])]

    for v in s[n:]:
        ws.append(v + ws[-1]*nf)

    return Series(ws, index=s.index)


@series_indicator('high')
def hhv(s, n):
    return s.rolling(n).max()
    # return pd.rolling_max(s, n)


@series_indicator('low')
def llv(s, n):
    return s.rolling(n).min()
    # return pd.rolling_min(s, n)


@series_indicator('close')
def ema(s, n, wilder=False):
    span = n if not wilder else 2*n - 1
    # sma = s.rolling(window=span, min_periods=span).mean()[:span]
    # rest = s[span:]
    # ewma = pd.concat([sma, rest]).ewm(span=span, adjust=False).mean()
    ewma = s.ewm(span=span).mean()
    return ewma


@series_indicator('close')
def ma(s, n):
    sma = s.rolling(window=n, min_periods=1).mean()
    return sma


@series_indicator('close')
def macd(s, nfast=12, nslow=26, nsig=9, percent=True):
    fast, slow = ema(s, nfast), ema(s, nslow)
    # fast, slow = s.ewm(span=nfast).mean(), s.ewm(span=nslow).mean()
    if percent:
        macd = 100*(fast / slow - 1)
    else:
        macd = fast - slow

    sig = ema(macd, nsig)
    # sig = macd.ewm(span=nsig).mean()
    hist = macd - sig

    return DataFrame(dict(macd=macd, signal=sig, hist=hist,
                          fast=fast, slow=slow))



def aroon(s, n=25):
    up = 100 * pd.rolling_apply(s.high, n + 1, lambda x: x.argmax()) / n
    dn = 100 * pd.rolling_apply(s.low, n + 1, lambda x: x.argmin()) / n

    return DataFrame(dict(up=up, down=dn))


@series_indicator('close')
def rsi(s, n=14):
    diff = s.diff()
    which_dn = diff < 0

    up, dn = diff, diff*0
    up[which_dn], dn[which_dn] = 0, -up[which_dn]

    emaup = ema(up, n, wilder=True)
    emadn = ema(dn, n, wilder=True)

    return 100 * emaup/(emaup + emadn)


def stoch(s, nfastk=14, nfullk=3, nfulld=3):
    if not isinstance(s, DataFrame):
        s = DataFrame(dict(high=s, low=s, close=s))

    hmax, lmin = hhv(s, nfastk), llv(s, nfastk)

    fastk = 100 * (s.close - lmin)/(hmax - lmin)
    # fullk = pd.rolling_mean(fastk, nfullk)
    # fulld = pd.rolling_mean(fullk, nfulld)
    fullk = fastk.rolling(nfullk).mean()
    fulld = fullk.rolling(nfulld).mean()

    return DataFrame(dict(fastk=fastk, fullk=fullk, fulld=fulld))


@series_indicator('close')
def dtosc(s, nrsi=13, nfastk=8, nfullk=5, nfulld=3):
    srsi = stoch(rsi(s, nrsi), nfastk, nfullk, nfulld)
    return DataFrame(dict(fast=srsi.fullk, slow=srsi.fulld))


def atr(s, n=14):
    cs = s.close.shift(1)
    tr = s.high.combine(cs, max) - s.low.combine(cs, min)

    return ema(tr, n, wilder=True)


def cci(s, n=20, c=0.015):
    if isinstance(s, DataFrame):
        s = s[['high', 'low', 'close']].mean(axis=1)

    mavg = pd.rolling_mean(s, n)
    mdev = pd.rolling_apply(s, n, lambda x: np.fabs(x - x.mean()).mean())

    return (s - mavg)/(c * mdev)


def cmf(s, n=20):
    clv = (2*s.close - s.high - s.low) / (s.high - s.low)
    vol = s.volume

    return pd.rolling_sum(clv*vol, n) / pd.rolling_sum(vol, n)


def force(s, n=2):
    return ema(s.close.diff()*s.volume, n)


@series_indicator('close')
def kst(s, r1=10, r2=15, r3=20, r4=30, n1=10, n2=10, n3=10, n4=15, nsig=9):
    rocma1 = pd.rolling_mean(s / s.shift(r1) - 1, n1)
    rocma2 = pd.rolling_mean(s / s.shift(r2) - 1, n2)
    rocma3 = pd.rolling_mean(s / s.shift(r3) - 1, n3)
    rocma4 = pd.rolling_mean(s / s.shift(r4) - 1, n4)

    kst = 100*(rocma1 + 2*rocma2 + 3*rocma3 + 4*rocma4)
    sig = pd.rolling_mean(kst, nsig)

    return DataFrame(dict(kst=kst, signal=sig))


def ichimoku(s, n1=9, n2=26, n3=52):
    conv = (hhv(s, n1) + llv(s, n1)) / 2
    base = (hhv(s, n2) + llv(s, n2)) / 2

    spana = (conv + base) / 2
    spanb = (hhv(s, n3) + llv(s, n3)) / 2

    return DataFrame(dict(conv=conv, base=base, spana=spana.shift(n2),
                          spanb=spanb.shift(n2), lspan=s.close.shift(-n2)))


def ultimate(s, n1=7, n2=14, n3=28):
    cs = s.close.shift(1)
    bp = s.close - s.low.combine(cs, min)
    tr = s.high.combine(cs, max) - s.low.combine(cs, min)

    avg1 = pd.rolling_sum(bp, n1) / pd.rolling_sum(tr, n1)
    avg2 = pd.rolling_sum(bp, n2) / pd.rolling_sum(tr, n2)
    avg3 = pd.rolling_sum(bp, n3) / pd.rolling_sum(tr, n3)

    return 100*(4*avg1 + 2*avg2 + avg3) / 7


def auto_envelope(s, nema=22, nsmooth=100, ndev=2.7):
    sema = ema(s.close, nema)
    mdiff = s[['high','low']].sub(sema, axis=0).abs().max(axis=1)
    csize = pd.ewmstd(mdiff, nsmooth)*ndev

    return DataFrame(dict(ema=sema, lenv=sema - csize, henv=sema + csize))


@series_indicator('close')
def bbands(s, n=20, ndev=2):
    mavg = pd.rolling_mean(s, n)
    mstd = pd.rolling_std(s, n)

    hband = mavg + ndev*mstd
    lband = mavg - ndev*mstd

    return DataFrame(dict(ma=mavg, lband=lband, hband=hband))


def safezone(s, position, nmean=10, npen=2.0, nagg=3):
    if isinstance(s, DataFrame):
        s = s.low if position == 'long' else s.high

    sgn = -1.0 if position == 'long' else 1.0

    # Compute the average upside/downside penetration
    pen = pd.rolling_apply(
        sgn*s.diff(), nmean,
        lambda x: x[x > 0].mean() if (x > 0).any() else 0
    )

    stop = s + sgn*npen*pen
    return hhv(stop, nagg) if position == 'long' else llv(stop, nagg)


def sar(s, af=0.02, amax=0.2):
    high, low = s.high, s.low

    # Starting values
    sig0, xpt0, af0 = True, high[0], af
    sar = [low[0] - (high - low).std()]

    for i in range(1, len(s)):
        sig1, xpt1, af1 = sig0, xpt0, af0

        lmin = min(low[i - 1], low[i])
        lmax = max(high[i - 1], high[i])

        if sig1:
            sig0 = low[i] > sar[-1]
            xpt0 = max(lmax, xpt1)
        else:
            sig0 = high[i] >= sar[-1]
            xpt0 = min(lmin, xpt1)

        if sig0 == sig1:
            sari = sar[-1] + (xpt1 - sar[-1])*af1
            af0 = min(amax, af1 + af)

            if sig0:
                af0 = af0 if xpt0 > xpt1 else af1
                sari = min(sari, lmin)
            else:
                af0 = af0 if xpt0 < xpt1 else af1
                sari = max(sari, lmax)
        else:
            af0 = af
            sari = xpt0

        sar.append(sari)

    return Series(sar, index=s.index)


def adx(s, n=14):
    cs = s.close.shift(1)
    tr = s.high.combine(cs, max) - s.low.combine(cs, min)
    trs = _wilder_sum(tr, n)

    up = s.high - s.high.shift(1)
    dn = s.low.shift(1) - s.low

    pos = ((up > dn) & (up > 0)) * up
    neg = ((dn > up) & (dn > 0)) * dn

    dip = 100 * _wilder_sum(pos, n) / trs
    din = 100 * _wilder_sum(neg, n) / trs

    dx = 100 * np.abs((dip - din)/(dip + din))
    adx = ema(dx, n, wilder=True)

    return DataFrame(dict(adx=adx, dip=dip, din=din))


def chandelier(s, position, n=22, npen=3):
    if position == 'long':
        return hhv(s, n) - npen*atr(s, n)
    else:
        return llv(s, n) + npen*atr(s, n)


def vortex(s, n=14):
    ss = s.shift(1)

    tr = s.high.combine(ss.close, max) - s.low.combine(ss.close, min)
    trn = pd.rolling_sum(tr, n)

    vmp = np.abs(s.high - ss.low)
    vmm = np.abs(s.low - ss.high)

    vip = pd.rolling_sum(vmp, n) / trn
    vin = pd.rolling_sum(vmm, n) / trn

    return DataFrame(dict(vin=vin, vip=vip))


@series_indicator('close')
def gmma(s, nshort=[3, 5, 8, 10, 12, 15],
         nlong=[30, 35, 40, 45, 50, 60]):
    short = {str(n): ema(s, n) for n in nshort}
    long = {str(n): ema(s, n) for n in nlong}

    return DataFrame(short), DataFrame(long)


def zigzag(s, pct=5):
    ut = 1 + pct / 100
    dt = 1 - pct / 100

    ld = s.index[0]
    lp = s.close[ld]
    tr = None

    zzd, zzp = [ld], [lp]

    for ix, ch, cl in zip(s.index, s.high, s.low):
        # No initial trend
        if tr is None:
            if ch / lp > ut:
                tr = 1
            elif cl / lp < dt:
                tr = -1
        # Trend is up
        elif tr == 1:
            # New high
            if ch > lp:
                ld, lp = ix, ch
            # Reversal
            elif cl / lp < dt:
                zzd.append(ld)
                zzp.append(lp)

                tr, ld, lp = -1, ix, cl
        # Trend is down
        else:
            # New low
            if cl < lp:
                ld, lp = ix, cl
            # Reversal
            elif ch / lp > ut:
                zzd.append(ld)
                zzp.append(lp)

                tr, ld, lp = 1, ix, ch

    # Extrapolate the current trend
    if zzd[-1] != s.index[-1]:
        zzd.append(s.index[-1])

        if tr is None:
            zzp.append(s.close[zzd[-1]])
        elif tr == 1:
            zzp.append(s.high[zzd[-1]])
        else:
            zzp.append(s.low[zzd[-1]])

    return Series(zzp, index=zzd)
