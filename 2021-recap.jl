using Dates 
periodstart = Date(2021, 1, 1);
periodend = Date(2021, 12, 31);

using Pkg

Pkg.add("CSV");
Pkg.add("DataFrames");
Pkg.add("MarketData");
Pkg.add("Glob");
Pkg.add("Plots");
Pkg.add("StatsPlots");
Pkg.add("PlotlyJS");
Pkg.add("PlotlyBase");
Pkg.add("WebIO");

using CSV
using DataFrames
using MarketData
using Glob
using Statistics

tradefiles = glob("data/*-trades.csv")

alltradesdf = reduce(vcat, DataFrame.(CSV.File.(tradefiles)));
alltradesdf[:,"VÄÄRTUSPÄEV"] = Date.(alltradesdf[:,"VÄÄRTUSPÄEV"])
alltradesdf[:,"TEHINGUPÄEV"] = Date.(alltradesdf[:,"TEHINGUPÄEV"])
alltradesdf = alltradesdf[in.(alltradesdf."TEHING", Ref(["ost", "müük"])), :]
# Replace temporary symbols
alltradesdf[alltradesdf."SÜMBOL" .== "EfTEN5", "SÜMBOL"] .= "EFT1T";

alltradesdf = sort!(alltradesdf, ["TEHINGUPÄEV"])

alltradesdf.CUMKOGUS .= 0
for sym in unique(alltradesdf[!,"SÜMBOL"])
    alltradesdf[alltradesdf."SÜMBOL" .== sym, "CUMKOGUS"] = cumsum(alltradesdf[alltradesdf."SÜMBOL" .== sym, "KOGUS"])
end
# alltradesdf[!, ["SÜMBOL", "TEHINGUPÄEV", "CUMKOGUS"]]

tickers = unique(alltradesdf[:,"SÜMBOL"])
function map_symbols(sym)
    if sym in ["SXR8", "VUSA"]
        return sym * ".DE"
    elseif sym in ["TVE1T", "MRK1T", "EFT1T", "TKM1T"]
        return sym * ".TL"
    elseif sym in ["GRG1L", "KNF1L"]
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

alltradesdf[:,"SÜMBOL"] = map(map_symbols, alltradesdf[:,"SÜMBOL"])

tickers = map(map_symbols, tickers)

currentyeardf = alltradesdf[(alltradesdf."TEHINGUPÄEV" .>= periodstart) .& (alltradesdf."TEHINGUPÄEV" .<= periodend), :]

prevtradesdf = alltradesdf[alltradesdf."TEHINGUPÄEV" .<= periodstart, :]
numcols = names(prevtradesdf, findall(x -> eltype(x) <: Number, eachcol(prevtradesdf)))
prevtradesdf = combine(groupby(prevtradesdf, ["SÜMBOL", "VALUUTA"]), numcols .=> sum .=> numcols)
prevtradesdf."TEHING" .= "ost"
prevtradesdf."TEHINGUPÄEV" .= periodstart
prevtradesdf."VÄÄRTUSPÄEV" .= periodstart
prevtradesdf."VÄÄRTPABER" .= "Dummy Value"
prevtradesdf."KOMMENTAAR" .= ""


prevtradesdf

currentyeardf = sort!(vcat(currentyeardf, prevtradesdf), ["TEHINGUPÄEV"])

currentyeardf[!, ["SÜMBOL", "TEHINGUPÄEV", "CUMKOGUS", "VALUUTA"]]

function download(ticker)
    data = yahoo(ticker, YahooOpt(period1=DateTime(periodstart)-Dates.Day(7), period2=DateTime(periodend), interval="1d"));
    df = DataFrame(data);
    df[!, "Ticker"] .= ticker;
    df
end
tickersdf = reduce(vcat, [download(ticker) for ticker in tickers])
tickersdf = tickersdf[completecases(tickersdf), :]

currencies = unique(currentyeardf."VALUUTA")
currencies = currencies[currencies .!= "EUR"]

function download_currency(ticker)
    data = yahoo(ticker * "EUR=X", YahooOpt(period1=DateTime(periodstart)-Dates.Day(7), period2=DateTime(periodend), interval="1d"));
    df = DataFrame(data);
    df[!, "Ticker"] .= ticker;
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
    currency = currentyeardf[range, "VALUUTA"][1]
    prices = tickersdf[(tickersdf."timestamp" .<= day) .& (tickersdf."Ticker" .== ticker), "Close"]
    if size(prices, 1) == 0
        return 0.0
    end
    price = last(prices)
        
    if currency == "EUR"
        return amount * price
    end
    
    rates = currenciesdf[(currenciesdf."timestamp" .<= day) .& (currenciesdf."Ticker" .== currency), "Close"]
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

# using Plots
using PlotlyJS

# plotlyjs()

lbls = [x for x in names(yearportfoliodf) if x != "Kuupäev"]
plotdf = stack(yearportfoliodf, lbls)
plotdf = DataFrames.rename(plotdf, ["Kuupäev", "Aktsia", "Väärtus"])
plotdf = plotdf[plotdf."Väärtus" .> 0,:]

layout = Layout(
    xaxis_title="Kuupäev",
    yaxis_title="Väärtus",
    legend_title_text="Aktsiad",
    xaxis=attr(dtick="M1", tickformat="%b", ticklabelmode="period", range=[periodstart, periodend]),
    plot_bgcolor="white",
    width=1800, height=1000,
)

PlotlyJS.plot(plotdf, x=:Kuupäev, y=:Väärtus, color=:Aktsia, legend = :outertopright, layout)


for c in currencies
    range = (alltradesdf."VALUUTA" .== c) .& (alltradesdf."TEHINGUPÄEV" .>= periodstart)
    prices = alltradesdf[range, "KOKKU"]
    days = alltradesdf[range, "TEHINGUPÄEV"]
    rates = [last(currenciesdf[(currenciesdf."timestamp" .<= d) .& (currenciesdf."Ticker" .== c), "Close"]) for d in days]
    alltradesdf[range, "KOKKU"] = prices .* rates
end

invested = -sum(alltradesdf[(alltradesdf."TEHINGUPÄEV" .>= periodstart) .& (alltradesdf."TEHINGUPÄEV" .<= periodend), "KOKKU"])

growth = sum(last(yearportfoliodf)[1:end-1]) - sum(first(yearportfoliodf)[1:end-1])

profit = growth - invested

plotdf = plotdf = yearportfoliodf[!, ["Kuupäev"]]
plotdf."KOKKU" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))

df = yearportfoliodf[!, ["Kuupäev"]]
df."KOKKU" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))
yearbuysdf = alltradesdf[(alltradesdf."TEHINGUPÄEV" .>= periodstart) .& (alltradesdf."TEHINGUPÄEV" .<= periodend), ["TEHINGUPÄEV", "KOKKU"]]
for row in eachrow(df)
    row."KOKKU" += yearbuysdf[row."Kuupäev" .>= yearbuysdf."TEHINGUPÄEV", "KOKKU"] |> sum
end

df2 = alltradesdf[(alltradesdf."TEHINGUPÄEV" .>= periodstart) .& (alltradesdf."TEHINGUPÄEV" .<= periodend), ["TEHINGUPÄEV", "KOKKU"]]
df2."KOKKU" = -df2."KOKKU"

layout = Layout(
    xaxis_title="Kuupäev",
    yaxis_title="Väärtus",
    xaxis=attr(dtick="M1", tickformat="%b\n%Y", ticklabelmode="period", range=[periodstart, periodend]),
    plot_bgcolor="white",
    width=1800, height=600,
    barmode="relative"
)
plot(
    [
     scatter(plotdf, x=:Kuupäev, y=:KOKKU, name="Portfell")
     scatter(df, x=:Kuupäev, y=:KOKKU, name="Naturaalne")
     bar(df2, x=:TEHINGUPÄEV, y=:KOKKU, name="Investeering")
    ],
    layout
)

profitlossfiles = glob("data/*-profit-loss.csv")

allprofitlossdf = reduce(vcat, DataFrame.(CSV.File.(profitlossfiles)));

show(describe(allprofitlossdf), allcols=true)

yearprofitlossdf = allprofitlossdf[(allprofitlossdf."Laekumise kuupäev" .>= periodstart) .& (allprofitlossdf."Laekumise kuupäev" .<= periodend),:]

totaldividend = yearprofitlossdf."Kokku EUR" |> sum

plotdf = DataFrame()
plotdf."Kuupäev" = yearprofitlossdf[!, "Laekumise kuupäev"]
plotdf."Kokku" = cumsum(yearprofitlossdf."Kokku EUR")

layout = Layout(
    xaxis_title="Kuupäev",
    yaxis_title="Väärtus",
    legend_title_text="Aktsiad",
    xaxis=attr(dtick="M1", tickformat="%b", ticklabelmode="period", range=[periodstart, periodend]),
    plot_bgcolor="white",
    width=1800, height=600,
)

PlotlyJS.plot(plotdf, x=:Kuupäev, y=:Kokku, layout)

avg_portfolio = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]])) |> mean
dividend_yield = totaldividend / avg_portfolio

df = DataFrame()
df."Kuupäev" = yearprofitlossdf."Laekumise kuupäev"
df."KOKKU" = yearprofitlossdf."Kokku EUR"

layout = Layout(
    xaxis_title="Kuupäev",
    yaxis_title="Väärtus",
    legend_title_text="Aktsiad",
    xaxis=attr(dtick="M1", tickformat="%b", ticklabelmode="period", range=[periodstart, periodend]),
    plot_bgcolor="white",
)

plot(bar(df, x=:Kuupäev, y=:KOKKU, name="Dividend/Intress"), layout)

df = yearportfoliodf[!, ["Kuupäev"]]
df."Kokku" = sum.(eachrow(yearportfoliodf[!, names(yearportfoliodf)[1:end-1]]))
yearbuysdf = alltradesdf[(alltradesdf."TEHINGUPÄEV" .>= periodstart) .& (alltradesdf."TEHINGUPÄEV" .<= periodend), ["TEHINGUPÄEV", "KOKKU"]]
for row in eachrow(df)
    row."Kokku" += yearbuysdf[row."Kuupäev" .>= yearbuysdf."TEHINGUPÄEV", "KOKKU"] |> sum
end

yearstd = std(df."Kokku")

yearstd / avg_portfolio

profit / avg_portfolio


