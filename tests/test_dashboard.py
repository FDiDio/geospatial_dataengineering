import pandas as pd
import pytest

from dashboard.queries import BLUES, COLD_ARM, LAYERS, NEUTRAL, WARM_ARM, colorize, hex_to_rgb, scale_colors


def rgb(color):
    return list(hex_to_rgb(color))


def test_diverging_scale_is_neutral_at_its_center_and_clipped_at_its_bounds():
    layer = LAYERS["temperature_anomaly_c"]  # -10 .. 0 .. 10
    colors = colorize(pd.Series([-50.0, -10.0, 0.0, 10.0, 50.0]), layer)

    assert [c[:3] for c in colors] == [rgb(COLD_ARM[0]), rgb(COLD_ARM[0]), rgb(NEUTRAL["light"]), rgb(WARM_ARM[-1]),
                                       rgb(WARM_ARM[-1])]  # fmt: skip
    assert all(c[3] == 220 for c in colors)


def test_asymmetric_diverging_range_keeps_the_midpoint_on_the_center():
    layer = LAYERS["temperature_max_c"]  # -40 .. 0 .. 45
    assert colorize(pd.Series([0.0]), layer)[0][:3] == rgb(NEUTRAL["light"])


def test_dark_theme_uses_the_dark_midpoint():
    assert NEUTRAL["dark"] in scale_colors(LAYERS["temperature_mean_c"], "dark")


def test_sequential_scale_goes_from_light_to_dark():
    layer = LAYERS["precipitation_mm"]
    low, high = colorize(pd.Series([0.0, 500.0]), layer)
    assert low[:3] == rgb(BLUES[0]) and high[:3] == rgb(BLUES[-1])


@pytest.mark.parametrize("key", list(LAYERS))
def test_every_layer_has_a_valid_scale(key):
    layer = LAYERS[key]
    assert layer.low < layer.high
    if layer.scale == "diverging":
        assert layer.low < layer.center < layer.high
