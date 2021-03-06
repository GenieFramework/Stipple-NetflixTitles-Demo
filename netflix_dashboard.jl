module NetflixDashboardData

using CSV, DataFrames, DataFramesMeta
using Dates

original_data = DataFrame()

function parse_date_added(strdates::Vector{Union{String,Missing}})
  date_added_format = DateFormat("U d, Y")
  map(strdates) do d
    ismissing(d) ? rand(2008:2021) : Date(strip(d), date_added_format) |> year
  end
end

function parse_durations(durations::Vector{Union{String}})
  minutes = Union{Int,Missing}[]
  seasons = Union{Int,Missing}[]

  for d in durations
    d = strip(d) |> lowercase

    if ismissing(d)
      push!(minutes, d)
      push!(seasons, d)
    elseif endswith(d, "min")
      d = parse(Int, replace(d, "min"=>"") |> strip)

      push!(minutes, d)
      push!(seasons, missing)
    elseif endswith(d, "seasons")
      d = parse(Int, replace(d, "seasons"=>"") |> strip)

      push!(minutes, missing)
      push!(seasons, d)
    elseif endswith(d, "season")
      d = parse(Int, replace(d, "season"=>"") |> strip)

      push!(minutes, missing)
      push!(seasons, d)
    else
      error("Wtf? $d")
    end
  end

  (minutes, seasons)
end

function load_data() :: DataFrame
  global original_data = CSV.File(joinpath("data", "netflix_titles.csv")) |> DataFrame

  original_data.date_added = parse_date_added(original_data[!, :date_added] |> Array)

  original_data.duration_minutes, original_data.duration_seasons = parse_durations(original_data[!, :duration] |> Array)

  original_data
end

function types() :: Vector{String}
  original_data[!, :type] |> Array |> unique! |> sort!
end

function types_count() :: Dict{String,Int}
  result = Dict{String,Int}()

  for t in types()
    result[t] = @where(NetflixDashboardData.original_data, :type .== t)[!, :type] |> Array |> length
  end

  result
end


function titles() :: Vector{String}
  original_data[!, :title] |> Array |> unique! |> sort!
end

function individuals(data::V)::Vector{V} where {V<:AbstractVector}
  result = V[]

  for d in data
    if occursin(',', d)
      push!(result, strip.(split(d, ','))...)
    else
      push!(result, strip(d))
    end
  end

  result |> unique! |> sort!
end

function directors() :: Vector{String}
  original_data[!, :director] |> Array |> individuals
end

function actors() :: Vector{String}
  original_data[!, :cast] |> Array |> individuals
end

function countries() :: Vector{String}
  original_data[!, :country] |> Array |> individuals
end

function categories() :: Vector{String}
  original_data[!, :listed_in] |> Array |> individuals
end

function durations_movies() :: Vector{Int}
  original_data[!, :duration_minutes] |> Array |> skipmissing |> collect
end

function durations_shows() :: Vector{Int}
  original_data[!, :duration_seasons] |> Array |> skipmissing |> collect
end

const years = [1925:2021...]

function releases_per_year() :: Tuple{Vector{Int},Vector{Int}}
  no_of_releases = Int[]

  releases_by_year = combine(groupby(original_data, [:release_year]), nrow => :count)

  for y in years
    r = releases_by_year[releases_by_year[!, :release_year] .== y, :count]
    push!(no_of_releases, isempty(r) ? 0 : r[1])
  end

  years, no_of_releases
end

function added_per_year() :: Tuple{Vector{Int},Vector{Int}}
  no_of_added = Int[]

  added_by_year = combine(groupby(original_data, [:date_added]), nrow => :count)

  for y in years
    r = added_by_year[added_by_year[!, :date_added] .== y, :count]
    push!(no_of_added, isempty(r) ? 0 : r[1])
  end

  years, no_of_added
end

function __init__() :: Nothing
  load_data()

  nothing
end

end # end NetflixDashboardData

#===#

module NetflixDashboard

using Genie, Stipple, StippleUI, StipplePlotly
using ..NetflixDashboardData

Base.@kwdef mutable struct Model <: ReactiveModel
  types_plot_data::R{PlotData} = PlotData(
    plot = StipplePlotly.Charts.PLOT_TYPE_PIE,
    hole = .4,
    values = values(NetflixDashboardData.types_count()) |> collect,
    labels = keys(NetflixDashboardData.types_count()) |> collect
  )
  types_plot_layout::R{PlotLayout} = PlotLayout(
    title_text = "Title Types"
  )

  releases_plot_data::R{Vector{PlotData}} = [PlotData(
    plot = StipplePlotly.Charts.PLOT_TYPE_SCATTER,
    mode = "lines",
    name = "Releases",
    x = NetflixDashboardData.releases_per_year()[1],
    y = NetflixDashboardData.releases_per_year()[2]
  ),
  PlotData(
    plot = StipplePlotly.Charts.PLOT_TYPE_SCATTER,
    mode = "lines",
    name = "Additions",
    x = NetflixDashboardData.added_per_year()[1],
    y = NetflixDashboardData.added_per_year()[2]
  )]
  releases_plot_layout::R{PlotLayout} = PlotLayout(
    title_text = "Release vs added times"
  )

  movies_durations_plot_data::R{PlotData} = PlotData(
    plot = StipplePlotly.Charts.PLOT_TYPE_HISTOGRAM,
    x = NetflixDashboardData.durations_movies(),
    name = "Movie duration in minutes"
  )
  movies_durations_plot_layout::R{PlotLayout} = PlotLayout(
    title_text = "Movie durations"
  )

  series_durations_plot_data::R{PlotData} = PlotData(
    plot = StipplePlotly.Charts.PLOT_TYPE_HISTOGRAM,
    x = NetflixDashboardData.durations_shows(),
    name = "Number of seasons"
  )
  series_durations_plot_layout::R{PlotLayout} = PlotLayout(
    title_text = "Series durations"
  )
end

model = Stipple.init(Model())

function ui(model::R) where {R<:ReactiveModel}
  [
    dashboard(
      vm(model),
      title="Netflix Titles Data Exploration",
      head_content=Genie.Assets.favicon_support(),
      partial=false,
      class="container",
      [
        heading("Netflix Titles")

        row(
          cell(class="st-module", [
            h4("A visual exploration of the Netflix titles catalog based on a free dataset containing 7539 entries.",
            class="text-muted",
            style="padding: 10px 20px; width: 100%;"),
          ])
        )

        row([
          cell(class="st-module", [
            plot(:types_plot_data, layout = :types_plot_layout)
          ])
          cell(class="st-module", [
            plot(:movies_durations_plot_data, layout = :movies_durations_plot_layout)
          ])
          cell(class="st-module", [
            plot(:series_durations_plot_data, layout = :series_durations_plot_layout)
          ])
        ])

        row(
          cell(class="st-module", [
            plot(:releases_plot_data, layout = :releases_plot_layout)
          ])
        )
      ]
    )
  ]
end

end # end NetflixDashboard

#===#

using Genie, Genie.Renderer.Html
using DataFrames
using .NetflixDashboardData
using .NetflixDashboard

# routes
route("/") do
  NetflixDashboard.ui(NetflixDashboard.model) |> html
end

# up(rand((8000:9000)), open_browser=true)