using Dates
periodstart = Date(2022, 1, 1);
periodend = Date(2022, 08, 31);

# using Pkg
#
# Pkg.add("CSV");
# Pkg.add("DataFrames");
# Pkg.add("MarketData");
# Pkg.add("Glob");
# Pkg.add("Plots");
# Pkg.add("StatsPlots");
# Pkg.add("PlotlyBase");
# Pkg.add("WebIO");

using CSV
using DataFrames
using MarketData
using Glob
using Statistics
using GLM

tradefiles = glob("data/*-trades.csv")

alltradesdf = reduce(vcat, DataFrame.(CSV.File.(tradefiles)));
alltradesdf[:, "VÄÄRTUSPÄEV"] = Date.(alltradesdf[:, "VÄÄRTUSPÄEV"]);
alltradesdf[:, "TEHINGUPÄEV"] = Date.(alltradesdf[:, "TEHINGUPÄEV"]);
alltradesdf = alltradesdf[in.(alltradesdf."TEHING", Ref(["ost", "müük"])), :]
# Replace temporary symbols
alltradesdf[alltradesdf."SÜMBOL".=="EfTEN5", "SÜMBOL"] .= "EFT1T";

alltradesdf = sort!(alltradesdf, ["TEHINGUPÄEV"])

alltradesdf.CUMKOGUS .= 0
for sym in unique(alltradesdf[!, "SÜMBOL"])
  alltradesdf[alltradesdf."SÜMBOL".==sym, "CUMKOGUS"] = cumsum(alltradesdf[alltradesdf."SÜMBOL".==sym, "KOGUS"])
end
# alltradesdf[!, ["SÜMBOL", "TEHINGUPÄEV", "CUMKOGUS"]]

tickers = unique(alltradesdf[:, "SÜMBOL"])
function map_symbols(sym)
  if sym in ["SXR8", "VUSA"]
    return sym * ".DE"
  elseif sym in ["TVE1T", "MRK1T", "EFT1T", "TKM1T", "SFG1T"]
    return sym * ".TL"
  elseif sym in ["GRG1L", "KNF1L", "SAB1L"]
    return sym * ".VS"
  elseif sym in ["GZE1R"]
    return sym * ".RG"
  elseif sym in ["LEO", "SWEDA"]
    if sym == "SWEDA"
      sym = "SWED-A"
    end
    return sym * ".ST"
  elseif sym == "LQDA"
    return "IBCD.DE"
  end
  sym
end

alltradesdf[:, "SÜMBOL"] = map(map_symbols, alltradesdf[:, "SÜMBOL"])

tickers = map(map_symbols, tickers)

currentyeardf = alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), :]

prevtradesdf = alltradesdf[alltradesdf."TEHINGUPÄEV".<=periodstart, :];
numcols = names(prevtradesdf, findall(x -> eltype(x) <: Number, eachcol(prevtradesdf)));
prevtradesdf = combine(groupby(prevtradesdf, ["SÜMBOL", "VALUUTA"]), numcols .=> sum .=> numcols);
prevtradesdf."TEHING" .= "ost";
prevtradesdf."TEHINGUPÄEV" .= periodstart;
prevtradesdf."VÄÄRTUSPÄEV" .= periodstart;
prevtradesdf."VÄÄRTPABER" .= "Dummy Value";
prevtradesdf."KOMMENTAAR" .= "";


prevtradesdf

currentyeardf = sort!(vcat(currentyeardf, prevtradesdf), ["TEHINGUPÄEV"]);

currentyeardf[!, ["SÜMBOL", "TEHINGUPÄEV", "CUMKOGUS", "VALUUTA"]]

function download(ticker)
  df = DataFrame("timestamp" => Date[], "Open" => Float64[], "High" =>
      Float64[], "Low" => Float64[], "Close" => Float64[],
    "AdjClose" => Float64[], "Volume" => Float64[], "Ticker" =>
      String[]);
  try
    data = yahoo(ticker, YahooOpt(period1=DateTime(periodstart) - Dates.Day(7), period2=DateTime(periodend), interval="1d"))
    df = DataFrame(data)
    df[!, "Ticker"] .= ticker
  catch
    print("Failed to download: ", ticker, "\n")
    tmp = alltradesdf[alltradesdf."SÜMBOL" .== ticker, :];
    for t in eachrow(tmp)
      v = t."HIND";
      push!(df, [t."VÄÄRTUSPÄEV" v v v v v 0 ticker]);
    end
  end
  df
end
tickersdf = reduce(vcat, [download(ticker) for ticker in tickers]);
tickersdf = tickersdf[completecases(tickersdf), :]

currencies = unique(currentyeardf."VALUUTA")
currencies = currencies[currencies.!="EUR"];

function download_currency(ticker)
  tstart = alltradesdf[1, "TEHINGUPÄEV"] - Dates.Day(7)
  df = DataFrame()
  data = yahoo(ticker * "EUR=X", YahooOpt(period1=DateTime(tstart), period2=DateTime(periodend), interval="1d"))
  df = DataFrame(data)
  df[!, "Ticker"] .= ticker
  df
end

currenciesdf = reduce(vcat, [download_currency(c) for c in currencies]);

tickercols = NamedTuple{Tuple([Symbol(t) for t in tickers])}(Float64[] for _ in tickers)
yearportfoliodf = DataFrame(tickercols)
yearportfoliodf[!, "Kuupäev"] = Date[]

function calc_day(ticker, day)
  range = (currentyeardf."TEHINGUPÄEV" .<= day) .& (currentyeardf."SÜMBOL" .== ticker)
  amount = sum(currentyeardf[range, "KOGUS"])
  if amount == 0
    return 0.0
  end
  prices = tickersdf[(tickersdf."timestamp".<=day).&(tickersdf."Ticker".==ticker), "Close"]
  if size(prices, 1) == 0
    return 0.0
  end
  price = last(prices)

  currency = currentyeardf[range, "VALUUTA"][1]
  if currency == "EUR"
    return amount * price
  end

  rates = currenciesdf[(currenciesdf."timestamp".<=day).&(currenciesdf."Ticker".==currency), "Close"]
  if size(rates, 1) == 0
    return 0.0
  end
  rate = last(rates)

  amount * price * rate
end

day = periodstart
while day < periodend
  row = [calc_day(t, day) for t in tickers]
  row = tuple(row..., day)
  push!(yearportfoliodf, row)
  day = day + Dates.Day(1)
end

yearportfoliodf

using Plots

# plotlyjs()

lbls = [x for x in names(yearportfoliodf) if x != "Kuupäev"];
plotdf = yearportfoliodf[!, lbls];
plotdf[!, lbls] = ifelse.(plotdf[!, lbls] .<= 0.0, missing, plotdf[!, lbls]);

plot(
  yearportfoliodf."Kuupäev",
  Matrix(plotdf),
  labels=permutedims(names(plotdf)),
  legend=:topleft,
  plot_title="Aktsiad",
  xlabel="Kuupäev",
  ylabel="Väärtus"
)


for c in currencies
  range = (alltradesdf."VALUUTA" .== c) .& (alltradesdf."TEHINGUPÄEV" .>= periodstart)
  prices = alltradesdf[range, "KOKKU"]
  days = alltradesdf[range, "TEHINGUPÄEV"]
  rates = [last(currenciesdf[(currenciesdf."timestamp".<=d).&(currenciesdf."Ticker".==c), "Close"]) for d in days]
  alltradesdf[range, "KOKKU"] = prices .* rates
end

invested = -sum(alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), "KOKKU"])

growth = sum(last(yearportfoliodf)[1:end-1]) - sum(first(yearportfoliodf)[1:end-1])

profit = growth - invested


profitlossfiles = glob("data/*-profit-loss.csv")

allprofitlossdf = reduce(vcat, DataFrame.(CSV.File.(profitlossfiles)));

show(describe(allprofitlossdf), allcols=true)

yearprofitlossdf = allprofitlossdf[(allprofitlossdf."Laekumise kuupäev".>=periodstart).&(allprofitlossdf."Laekumise kuupäev".<=periodend), :]

totaldividend = yearprofitlossdf."Kokku EUR" |> sum

plotdf = DataFrame()
plotdf."Kuupäev" = yearprofitlossdf[!, "Laekumise kuupäev"]
plotdf."Kokku" = cumsum(yearprofitlossdf."Kokku EUR")

plot(
  plotdf."Kuupäev",
  plotdf."Kokku",
  plot_title="Dividend",
  xlabel="Kuupäev",
  ylabel="Väärtus"
)

avg_portfolio = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]])) |> mean
dividend_yield = totaldividend / avg_portfolio

bar(
  yearprofitlossdf."Laekumise kuupäev",
  yearprofitlossdf."Kokku EUR",
  plot_title="Dividend/Intress",
  xlabel="Kuupäev",
  ylabel="Väärtus",
  legend=false
)

df = yearportfoliodf[!, ["Kuupäev"]]
df."Kokku" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))
yearbuysdf = alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), ["TEHINGUPÄEV", "KOKKU"]]
for row in eachrow(df)
  row."Kokku" += yearbuysdf[row."Kuupäev".>=yearbuysdf."TEHINGUPÄEV", "KOKKU"] |> sum
end

yearstd = std(df."Kokku")

yearstd / avg_portfolio

profit / avg_portfolio

plotdf = plotdf = yearportfoliodf[!, ["Kuupäev"]]
plotdf."KOKKU" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))

df = yearportfoliodf[!, ["Kuupäev"]]
df."KOKKU" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))
yearbuysdf = alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), ["TEHINGUPÄEV", "KOKKU"]]
for row in eachrow(df)
  row."KOKKU" += yearbuysdf[row."Kuupäev".>=yearbuysdf."TEHINGUPÄEV", "KOKKU"] |> sum
end

df2 = alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), ["TEHINGUPÄEV", "KOKKU"]]
df2."KOKKU" = -df2."KOKKU"

p = plot(
  plotdf."Kuupäev", plotdf."KOKKU",
  label="Portfell",
  legend=:topleft,
)

p = plot(
  p,
  df."Kuupäev", df."KOKKU",
  label="Naturaalne",
  legend=:topleft,
)

p = bar(
  p,
  df2."TEHINGUPÄEV", df2."KOKKU",
  label="Investeering",
)

p = bar(
  p,
  yearprofitlossdf."Laekumise kuupäev",
  .-yearprofitlossdf."Kokku EUR",
  label="Dividend/intress",
  legend=:topleft,
  plot_title="Aktsiad",
  xlabel="Kuupäev",
  ylabel="Väärtus",
  color=:red
)

# Plot only money put in and taken out
p = bar(
  df2."TEHINGUPÄEV", df2."KOKKU",
  label="Investeering",
)

p = bar(
  p,
  yearprofitlossdf."Laekumise kuupäev",
  .-yearprofitlossdf."Kokku EUR",
  label="Dividend/intress",
  legend=:topleft,
  plot_title="Aktsiad",
  xlabel="Kuupäev",
  ylabel="Väärtus",
  color=:red
)

# Cov, std, and more statistical stuff
sddf = yearportfoliodf[!, ["Kuupäev"]]
sddf."Kokku" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))
yearbuysdf = alltradesdf[(alltradesdf."TEHINGUPÄEV".>=periodstart).&(alltradesdf."TEHINGUPÄEV".<=periodend), ["TEHINGUPÄEV", "KOKKU"]]
for row in eachrow(sddf)
  row."Kokku" += yearbuysdf[row."Kuupäev".>=yearbuysdf."TEHINGUPÄEV", "KOKKU"] |> sum
end

lbls = [x for x in names(yearportfoliodf) if x != "Kuupäev"];
df = DataFrame("Ticker" => String[], "Std" => Float64[], "Value" => Float64[], "PBeta" => Float64[], "Change" => Float64[]);
for lbl in lbls
  ticker = tickersdf[tickersdf."Ticker".==lbl, ["timestamp", "Close"]]
  yearvalues = sddf[in.(sddf."Kuupäev", Ref(ticker."timestamp")), ["Kuupäev", "Kokku"]]
  ticker = ticker[in.(ticker."timestamp", Ref(yearvalues."Kuupäev")), :]
  prices = ticker."Close"
  sd = std(prices) / mean(prices)
  val = last(yearportfoliodf)[lbl]
  beta = cor(yearvalues."Kokku", prices)
  change = last(prices) / first(prices)
  push!(df, [lbl, sd, val, beta, change])
end

p = scatter(
  df."Value",
  df."Change",
  legend=false,
  plot_title="Portfelli jaotus",
  xlabel="Väärtus",
  ylabel="Muutus",
);
annotate!.(df."Value" .+ 100, df."Change", text.(lbls, :black, :left, 4));
p

# All time portfolio layout
df = DataFrame("Ticker" => String[], "Std" => Float64[], "Value" => Float64[],
               "Change" => Float64[], "Dividends" => Float64[],
               "TotalChange" => Float64[]);
soldtickers = String[];
for lbl in tickers
  ticker = tickersdf[tickersdf."Ticker".==lbl, ["timestamp", "Close"]]

  tradedf = alltradesdf[alltradesdf."SÜMBOL".==lbl, :]
  currency = tradedf[1, "VALUUTA"]
  buyprice = 0.0
  sellprice = 0.0

  if currency == "EUR"
    buyprice = -sum(tradedf[tradedf."TEHING".=="ost", "NETOSUMMA"])
    sellprice = sum(tradedf[tradedf."TEHING".=="müük", "NETOSUMMA"])
  else
    for row in eachrow(tradedf)
      rates = currenciesdf[(currenciesdf."timestamp".<=row."TEHINGUPÄEV").&(currenciesdf."Ticker".==currency), "Close"]
      rate = last(rates)
      if row."TEHING" == "ost"
        buyprice += rate * row."HIND" * row."KOGUS"
      else
        sellprice += -rate * row."HIND" * row."KOGUS"
      end
    end
  end

  range = [map_symbols(x[1]) == lbl for x in split.(allprofitlossdf."Väärtpaber", " - ")];
  dividends = allprofitlossdf[range, "Kokku EUR"] |> sum;

  valinhand = yearportfoliodf[end, lbl]

  if valinhand <= 0.0
    push!(soldtickers, lbl)
  end

  sellprice += valinhand;

  yearvalues = sddf[in.(sddf."Kuupäev", Ref(ticker."timestamp")), ["Kuupäev", "Kokku"]];
  ticker = ticker[in.(ticker."timestamp", Ref(yearvalues."Kuupäev")), :];
  prices = ticker."Close";
  sd = std(prices) / mean(prices);
  val = buyprice;
  change = sellprice / buyprice;
  totalchange = (sellprice + dividends) / buyprice;
  push!(df, [lbl, sd, val, change, dividends, totalchange]);
end

solddf = df[in.(df."Ticker", Ref(soldtickers)), :]
keptdf = df[.!in.(df."Ticker", Ref(soldtickers)), :]

p = scatter(
  keptdf."Value",
  keptdf."TotalChange",
  legend=false,
  plot_title="Portfelli jaotus",
  xlabel="Väärtus",
  ylabel="Muutus",
);

p = scatter(
  p,
  solddf."Value",
  solddf."TotalChange",
  legend=false,
  plot_title="Portfelli jaotus",
  xlabel="Väärtus",
  ylabel="Muutus",
  color=:red,
);

annotate!.(df."Value" .+ 100, df."TotalChange", text.(tickers, :black, :left, 4));
p

