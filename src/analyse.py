import pandas as pd
from src.data import Data
from src.tables import TData


class Analysis:

    def __init__(self, force_update=False):
        self.data_ = Data(force_update=force_update)
        self.cat = self.data_.cat

        self.date_cols = [self.data_.date.dt.year, self.data_.date.dt.month]

    @property
    def data(self):
        return self.data_.query('category != "Exclude"')

    def categorise(self, show_sub_cat=False, show_month=False, axis='index', bkg=False):
        cat_cols = self.cat.COLS if show_sub_cat else self.cat.COLS[:1]
        date_cols = self.date_cols if show_month else self.date_cols[:1]
        df = self.data.groupby(date_cols + cat_cols)['amount'].sum()
        date_names = ['year'] + (['month'] if show_month else [])
        df = df.unstack(cat_cols).sort_index(axis=1).rename_axis(date_names)
        idx_tot = ('total', '') if show_month else 'total'
        df.loc[idx_tot, :] = df.sum()
        return df
        dfs = df.style.format('{:,.0f}', na_rep='').set_caption('Expenses')
        if bkg:
            ...
        subset = pd.IndexSlice[df.index != idx_tot, ~df.columns.str.contains('Income')]
        return (df.style.format('{:,.0f}', na_rep='')
                .background_gradient(cmap='Blues_r', axis=axis, subset=subset))  # noqa

    def show_subcat(self, name):
        df = self.data.query(f'sub_category == "{name}"')
        cols = TData.TYPE_ORDER[::-1] + [df.date.dt.year, df.date.dt.month]
        df = df.set_index(cols)[['amount']]
        df.index.names = TData.TYPE_ORDER[::-1] + ['year', 'month']
        df = df.sort_index()
        return df.style.format('{:,.0f}', na_rep='').set_caption(f'Expenses in {name}')
