from plotnine import ggplot, aes, geom_point, geom_smooth, facet_grid, scale_x_log10, xlab, ylab, theme, element_text, element_blank, element_rect, element_line, ggtitle
from plotnine.themes import theme_minimal

(
    ggplot(gapminder, aes(x='gdpPercap', y='lifeExp', color='continent'))
    + geom_point(alpha=0.5, size=3)
    + facet_grid('. ~ continent')
    + geom_smooth(method='loess', color="black", size=1, se=False)
    + scale_x_log10(labels=lambda l: ["$10^%d$" % np.floor(np.log10(x)) for x in l])
    + ylab("Life Expectancy in Years")
    + xlab("GDP per Capita")
    + theme_minimal()
    + ggtitle("The relationship between wealth and longevity")
)

