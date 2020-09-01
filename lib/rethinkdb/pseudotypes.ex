defmodule RethinkDB.Pseudotypes do
  @moduledoc false
  defmodule Binary do
    @moduledoc false
    defstruct data: nil

    def parse(%{"$reql_type$" => "BINARY", "data" => data}, opts) do
      case Keyword.get(opts, :binary_format) do
        :raw ->
          %__MODULE__{data: data}

        _ ->
          :base64.decode(data)
      end
    end
  end

  defmodule Geometry do
    @moduledoc false
    defmodule Point do
      @moduledoc false
      defstruct coordinates: []
    end

    defmodule Line do
      @moduledoc false
      defstruct coordinates: []
    end

    defmodule Polygon do
      @moduledoc false
      defstruct coordinates: []
    end

    def parse(%{"$reql_type$" => "GEOMETRY", "coordinates" => [x, y], "type" => "Point"}) do
      %Point{coordinates: {x, y}}
    end

    def parse(%{"$reql_type$" => "GEOMETRY", "coordinates" => coords, "type" => "LineString"}) do
      %Line{coordinates: Enum.map(coords, &List.to_tuple/1)}
    end

    def parse(%{"$reql_type$" => "GEOMETRY", "coordinates" => coords, "type" => "Polygon"}) do
      %Polygon{coordinates: for(points <- coords, do: Enum.map(points, &List.to_tuple/1))}
    end
  end

  defmodule Time do
    @moduledoc false
    defstruct epoch_time: nil, timezone: nil

    def parse(
          %{"$reql_type$" => "TIME", "epoch_time" => epoch_time, "timezone" => timezone},
          opts
        ) do
      case Keyword.get(opts, :time_format) do
        :raw ->
          %__MODULE__{epoch_time: epoch_time, timezone: timezone}

        _ ->
          with <<sign::binary-size(1)>> <> rest = timezone <> ":00",
               {:ok, {h, m, _, _}} = Calendar.ISO.parse_time(rest) do
            sec = (h * 60 + m) * 60

            zone_abbr =
              case sec do
                0 -> "UTC"
                _ -> timezone
              end

            time_zone =
              case {div(sec, 3600), rem(sec, 3600)} do
                {0, 0} ->
                  "Etc/UTC"

                {hours, 0} ->
                  "Etc/GMT" <> sign <> Integer.to_string(hours)

                {hours, seconds} ->
                  "Etc/GMT" <>
                    sign <>
                    Integer.to_string(hours) <>
                    ":" <> String.pad_leading(Integer.to_string(seconds), 2, "0")
              end

            (epoch_time * 1000)
            |> trunc
            |> DateTime.from_unix!(:millisecond)
            |> struct(utc_offset: sec, zone_abbr: zone_abbr, time_zone: time_zone)
          end
      end
    end
  end

  def convert_reql_pseudotypes(nil, _opts), do: nil

  def convert_reql_pseudotypes(%{"$reql_type$" => "BINARY"} = data, opts) do
    Binary.parse(data, opts)
  end

  def convert_reql_pseudotypes(%{"$reql_type$" => "GEOMETRY"} = data, _opts) do
    Geometry.parse(data)
  end

  def convert_reql_pseudotypes(%{"$reql_type$" => "GROUPED_DATA"} = data, _opts) do
    parse_grouped_data(data)
  end

  def convert_reql_pseudotypes(%{"$reql_type$" => "TIME"} = data, opts) do
    Time.parse(data, opts)
  end

  def convert_reql_pseudotypes(list, opts) when is_list(list) do
    Enum.map(list, fn data -> convert_reql_pseudotypes(data, opts) end)
  end

  def convert_reql_pseudotypes(map, opts) when is_map(map) do
    Enum.map(map, fn {k, v} ->
      {k, convert_reql_pseudotypes(v, opts)}
    end)
    |> Enum.into(%{})
  end

  def convert_reql_pseudotypes(string, _opts), do: string

  def parse_grouped_data(%{"$reql_type$" => "GROUPED_DATA", "data" => data}) do
    Enum.map(data, fn [k, data] ->
      {k, data}
    end)
    |> Enum.into(%{})
  end

  def create_grouped_data(data) when is_map(data) do
    data = data |> Enum.map(fn {k, v} -> [k, v] end)
    %{"$reql_type$" => "GROUPED_DATA", "data" => data}
  end
end
