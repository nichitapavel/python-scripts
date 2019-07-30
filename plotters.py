from datetime import timedelta

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FuncFormatter


class CfgLabel:
    # Original code from
    # Author: Alberto Cabrera
    title = 'title'
    title_format = 'title_settings'
    xlabel = 'title_xaxis'
    ylabel = 'title_yaxis'
    ylabel2 = 'title_yaxis2'
    zlabel = 'title_zaxis'
    fname = 'filename'
    ftype = 'filetype'

    aspect_ratio = 'aspect_ratio'
    size = 'figure_size'
    dpi = 'dpi'

    steps = 'steps'

    # Legends
    legend_loc = 'legend_loc'
    legend_anchor = 'legend_anchor'
    show_legend = 'show_legend'
    line_legend = 'line_legend'
    line_legend2 = 'line_legend2'
    scatter_legend = 'scatter_legend'
    scatter_legend2 = 'scatter_legend2'

    # ticks
    tick_m = 'tick_multiple'
    subtick_m = 'subtick_multiple'
    ticks_format = 'ticks_settings'

    # colors
    cmap = 'color_map_3d'
    colors = 'colors20'
    # labels
    xticklabels = 'xticklabels'
    label_format = 'label_settings'

    # plot lines
    line_format = 'line_settings'

    # Markers 2d
    marker_format = 'marker_settings'

    # text
    text_format = 'text_settings'

    # background
    grid_format = 'grid_settings'
    grid_format2 = 'grid_settings2'
    spines = 'show_spines'

    # Layout
    layout_rect = 'layout_rect'


def thousands_to_k(x, pos=None):
    x = int(divmod(x, 1000)[0])
    return f'{x}k'


def get_yaxis_upper_value(yax_max, step=2000):
    max_value = 0
    yax_max_int = round(yax_max)
    for i in range(max_value, yax_max_int, step):
        if max_value < yax_max_int:
            max_value += step
    return max_value


def set_marks(axes, x_data, marks):
    """
    Draws vertical red lines with associated text on the plot
    :param axes: the plot area
    :param x_data: all data for the x axis, used to extract the x mark where to draw on plot
    :param marks: a list of tuples, where 1st item from tuple is the position
    of the x mark in 'x_data' and 2nd is the text associated with it
    :return:
    """
    i = 0
    for p, s in marks:
        line = plt.axvline(x=x_data[p])
        line.set_color('red')
        # Currently we consider that the marks will always come in pair:
        # start and finish, we use the number of the appearance to decide
        # which sits higher or lower, this is done to avoid text superposing
        if i % 2 == 0:
            y_upper_limit = axes.get_ylim()[1] * 0.9
        else:
            y_upper_limit = axes.get_ylim()[1] * 0.8
        plt.text(x=x_data[p], y=y_upper_limit, s=s, fontsize=16)
        i += 1


def set_x_as_date(ax, data):
    ax.set_xlim(left=data[0], right=data[-1])
    diff = data[-1] - data[0]
    # Set ticks and strings on X axes
    # Less than 5 minutes
    if diff <= timedelta(minutes=5):
        major_x_locator = mdates.SecondLocator(interval=10)
        minor_x_locator = mdates.SecondLocator(interval=5)
        date_format = mdates.DateFormatter('%Mm:%Ss')
    # Between 5 minutes and 10 minutes
    elif timedelta(minutes=5) < diff <= timedelta(minutes=10):
        major_x_locator = mdates.SecondLocator(interval=30)
        minor_x_locator = mdates.SecondLocator(interval=15)
        date_format = mdates.DateFormatter('%Mm:%Ss')
    # Between 10 minutes and 30 minutes
    elif timedelta(minutes=10) < diff <= timedelta(minutes=30):
        major_x_locator = mdates.MinuteLocator(interval=4)
        minor_x_locator = mdates.MinuteLocator(interval=2)
        date_format = mdates.DateFormatter('%Mm')
    # More than 30 minutes
    else:
        major_x_locator = mdates.MinuteLocator(interval=30)
        minor_x_locator = mdates.MinuteLocator(interval=15)
        date_format = mdates.DateFormatter('%Mm')

    ax.xaxis.set_major_locator(major_x_locator)
    ax.xaxis.set_minor_locator(minor_x_locator)

    x_axis_format = date_format
    ax.xaxis.set_major_formatter(x_axis_format)


def power_plot(filename, x_axis, y_axis, marks):
    config = {
        CfgLabel.ylabel: "Potencia (mW)",
        CfgLabel.aspect_ratio: 2.0,
        CfgLabel.size: 20,
        CfgLabel.fname: f'power_plot_{filename}',
        CfgLabel.ftype: 'png',
        CfgLabel.xlabel: "Tiempo"
    }

    fig = plt.figure(
        figsize=(
            config[CfgLabel.size],
            config[CfgLabel.size] / config[CfgLabel.aspect_ratio])
    )
    ax = fig.gca()  # Get Current Axes
    ax.grid(True, which='both')
    ax.set_ylabel(config[CfgLabel.ylabel])
    ax.set_xlabel(config[CfgLabel.xlabel])
    ax.set_ylim(
        0,
        get_yaxis_upper_value(max(y_axis))
    )

    # Set ticks and strings on Y axes
    ax.yaxis.set_major_locator(MultipleLocator(1000))
    ax.yaxis.set_minor_locator(MultipleLocator(500))
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_to_k))

    # The upper-right coordinate is calculated by the sum of ([0] + [2]. [1] + [3])
    # in this case the lower-left coordinate is (0.05, 0.06) and the upper-right is
    # (0.05 + 0.92, 0.06 + 0.9) which equals to (0.97, 0.96)
    ax.set_position(pos=[0.06, 0.09, 0.91, 0.85])

    # configure the x axis locators and ticks format
    set_x_as_date(ax, x_axis)

    plt.plot(x_axis, y_axis)
    set_marks(ax, x_axis, marks)

    # ******************** CAMBIAR TAMAÑO TEXTO ********************
    # ax.axes.title.set_fontsize(25)
    # ax.xaxis.label.set_fontsize(20)
    # ax.yaxis.label.set_fontsize(20)
    #
    # ticklabels = ax.get_xticklabels()
    # for item in ticklabels:
    #     item.set_fontsize(20)
    #
    # ticklabels = ax.get_yticklabels()
    # for item in ticklabels:
    #     item.set_fontsize(20)
    # ******************** CAMBIAR TAMAÑO TEXTO ********************

    # plt.show()
    plt.savefig(f'{config[CfgLabel.fname]}.{config[CfgLabel.ftype]}')
    plt.close(fig)


def box_plot(filename, label, data):
    config = {
        CfgLabel.ylabel: label,
        CfgLabel.xlabel: '# Hilos',
        CfgLabel.title: f'Variation of {label}',
        CfgLabel.aspect_ratio: 1.0,
        CfgLabel.dpi: 100,
        CfgLabel.size: 5,
        CfgLabel.fname: f'boxplot-{label}-f{filename}'
    }

    fig, ax = plt.subplots(
        figsize=(
            config[CfgLabel.size],
            config[CfgLabel.size] / config[CfgLabel.aspect_ratio])
    )
    ax.set_xlabel(config[CfgLabel.xlabel])
    ax.set_ylabel(config[CfgLabel.ylabel])

    # ******************** CAMBIAR TAMAÑO TEXTO ********************
    # ax.axes.title.set_fontsize(25)
    # ax.xaxis.label.set_fontsize(20)
    # ax.yaxis.label.set_fontsize(20)
    #
    # ticklabels = ax.get_xticklabels()
    # for item in ticklabels:
    #     item.set_fontsize(20)
    #
    # ticklabels = ax.get_yticklabels()
    # for item in ticklabels:
    #     item.set_fontsize(20)
    # ******************** CAMBIAR TAMAÑO TEXTO ********************
    ax.boxplot(data.values(), labels=data.keys())
    plt.savefig(fig)
    plt.close(fig)


if __name__ == '__main__':
    print('This is not meant to be executed')
