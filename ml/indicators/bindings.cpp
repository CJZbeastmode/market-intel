#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "indicators.cpp"

namespace py = pybind11;

PYBIND11_MODULE(indicators, m) {
    m.doc() = "Fast C++ technical indicators";
    m.def("rsi", &rsi, "Relative Strength Index", py::arg("prices"), py::arg("period") = 14);
    m.def("sma", &sma, "Simple Moving Average", py::arg("prices"), py::arg("period"));
    m.def("ema", &ema, "Exponential Moving Average", py::arg("prices"), py::arg("period"));
    m.def(
        "atr",
        &atr,
        "Average True Range",
        py::arg("highs"),
        py::arg("lows"),
        py::arg("closes"),
        py::arg("period") = 14
    );
    m.def(
        "bollinger",
        &bollinger,
        "Bollinger Bands",
        py::arg("prices"),
        py::arg("period") = 20,
        py::arg("stddev_multiplier") = 2.0
    );
    m.def("obv", &obv, "On-Balance Volume", py::arg("closes"), py::arg("volumes"));

    py::class_<MACDResult>(m, "MACDResult")
        .def_readonly("macd", &MACDResult::macd)
        .def_readonly("signal", &MACDResult::signal)
        .def_readonly("histogram", &MACDResult::histogram);

    m.def(
        "macd",
        &macd,
        "Moving Average Convergence/Divergence",
        py::arg("prices"),
        py::arg("fast") = 12,
        py::arg("slow") = 26,
        py::arg("signal_period") = 9
    );

    py::class_<BollingerResult>(m, "BollingerResult")
        .def_readonly("upper", &BollingerResult::upper)
        .def_readonly("middle", &BollingerResult::middle)
        .def_readonly("lower", &BollingerResult::lower);
}
