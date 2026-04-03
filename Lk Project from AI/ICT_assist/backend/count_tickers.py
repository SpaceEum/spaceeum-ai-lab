import ccxt.async_support as ccxt
import asyncio

async def main():
    exchange = ccxt.binanceusdm()
    try:
        tickers = await exchange.fetch_tickers()
        usdt_symbols = []
        for symbol in tickers:
            if symbol.endswith(':USDT') or symbol.endswith('/USDT'):
                usdt_symbols.append(symbol)
        print(f"Total USDT Pairs: {len(usdt_symbols)}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
