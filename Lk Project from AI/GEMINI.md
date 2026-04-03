# Gemini CLI 사용 안내

## 기본 설정

-   **언어:** 모든 답변은 한글로 제공해야 합니다.

## 백테스트 결과 저장 요구사항

백테스트 결과는 다음 명세에 따라 `result_(날짜_시간).db` 형식의 데이터베이스 파일에 저장되어야 합니다.

### 포함되어야 할 필드

-   티커 (Ticker)
-   수익률 (Return)
-   손익비 (Profit/Loss Ratio)
-   승률 (Win Rate)
-   손절 (Stop Loss)
-   익절 (Take Profit)
-   트레일링스탑 (Trailing Stop)
