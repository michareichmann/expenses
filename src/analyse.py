import pandas as pd
import plotly.express as px
from src.data import Data
from src.tables import TData
from datetime import datetime


class Analysis:

    def __init__(self, force_update=False):
        self.data_ = Data(force_update=force_update)
        self.cat = self.data_.cat

        self.date_cols = [self.data_.date.dt.year, self.data_.date.dt.month]

    @property
    def data(self):
        return self.data_.query('category != "Exclude"')

    def categorise(self, show_sub_cat=False, show_month=False):
        cat_cols = self.cat.COLS if show_sub_cat else self.cat.COLS[:1]
        date_cols = self.date_cols if show_month else self.date_cols[:1]
        df = self.data.groupby(date_cols + cat_cols)['amount'].sum()
        date_names = ['year'] + (['month'] if show_month else [])
        df = df.unstack(cat_cols).sort_index(axis=1).rename_axis(date_names)
        idx_tot = ('total', '') if show_month else 'total'
        df.loc[idx_tot, :] = df.sum()
        return df

    def show_categories(self, show_month=False, bkg=False, axis=0):
        df = self.categorise(show_sub_cat=False, show_month=show_month)
        caption = f'{"Montly" if show_month else "Yearly"} Expenses'
        dfs = df.style.format('{:,.0f}', na_rep='').set_caption(caption)
        if bkg:
            subset = pd.IndexSlice[df.index[:-1], ~df.columns.str.contains('Income')]
            dfs = (dfs.format('{:,.0f}', na_rep='')
                   .background_gradient(cmap='Blues_r', axis=axis, subset=subset))  # noqa
        return dfs

    def show_subcats(self, show_month=False, bkg=False, axis=1):
        df = self.categorise(show_sub_cat=True, show_month=show_month).T
        caption = f'{"Montly" if show_month else "Yearly"} Expenses by Sub-category'
        dfs = df.style.format('{:,.0f}', na_rep='').set_caption(caption)
        if bkg:
            idx = ~df.index.get_level_values(0).str.contains('Income')
            subset = pd.IndexSlice[idx, df.columns[:-1]]
            dfs = (dfs.format('{:,.0f}', na_rep='')
                   .background_gradient(cmap='Blues_r', axis=axis, subset=subset))  # noqa
        return dfs

    def plot_category(self, cat=None, sub_cat=None, show_month=False):
        df = self.categorise(show_sub_cat=sub_cat is not None, show_month=show_month)
        df = df.abs().iloc[:-1]
        if sub_cat is not None:
            df = df.droplevel('category', axis=1)

        if show_month:
            x = df.index.map(lambda i: datetime(year=i[0], month=i[1], day=1))
        else:
            x = df.index
        y = sub_cat or cat
        title = f'{"Monthly" if show_month else "Yearly"} {y} Expenses'
        fig = px.scatter(df, x=x, y=y or sub_cat, title=title)

        # Update layout
        time_period = "Monthly" if show_month else "Yearly"
        fig.update_traces(mode='lines+markers', line=dict(width=2), marker=dict(size=8))
        fig.update_layout(
            xaxis_title="Date" if show_month else "Year",
            yaxis_title="Amount [PLN]",
            hovermode='x unified',
            template='plotly_white'
        )
        return fig

    @staticmethod
    def format_cat(df: pd.DataFrame):
        cols = TData.TYPE_ORDER[::-1] + [df.date.dt.year, df.date.dt.month]
        df = df.set_index(cols)[['amount']]
        df.index.names = TData.TYPE_ORDER[::-1] + ['year', 'month']
        df = df.sort_index()
        return df.style.format('{:,.0f} zł', na_rep='')

    def show_subcat(self, name):
        df = self.data.query(f'sub_category == "{name}"')
        return self.format_cat(df).set_caption(f'Expenses in {name}')

    def show_uncategorised(self, n=None):
        df = self.data_.uncategorised.head(n)
        return self.format_cat(df).set_caption('Uncategorised Expenses')

