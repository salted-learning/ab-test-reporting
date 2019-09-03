# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 14:51:05 2019

@author: michael.schulte
"""

import multiprocessing

import numpy as np
import pandas as pd
import scipy.stats as stats
import statsmodels.api as sm


QUANTILES = np.arange(.1, 1, .2)


class ContinuousTestEval:
    def __init__(self, control, test):
        self.control = control
        self.test = test


    def __repr__(self):
        return 'Class for A/B testing on continuous data'


    @property
    def data_prep(self):
        '''Convert data to numpy arrays for consistency'''
        if type(self.control) != np.ndarray:
            self.control = np.array(self.control)
        if type(self.test) != np.ndarray:
            self.test = np.array(self.control)

        return self.control, self.test


    def _multiprocess_pval_loop(self, *args):
        '''

        :param df:
        :return:
        '''
        ctrl_boot = args[0][0].sample(args[0][1], replace=True)
        test_boot = args[0][0].sample(args[0][2], replace=True)
        boot_t = np.abs(stats.ttest_ind(ctrl_boot, test_boot)[0])

        return boot_t


    def continuous_pval(self, n = 1000):
        '''Bootstrapped p-value on continous variable using permutation method
        ----------
        Params:
            control: continuous data array for control group
            test = continuous data array for test group
            n = number of bootstrap iterations (higher is more accurate, more computationally expensive)
        '''
        control, test = self.data_prep

        t_stat = stats.ttest_ind(control, test)[0]
        df = pd.DataFrame({'data': np.append(control, test)})

        p = multiprocessing.Pool()
        diff = p.map(self._multiprocess_pval_loop, [(df, control.shape[0], test.shape[0]) for i in range(n)])

        p_val = np.mean(np.where(np.abs(t_stat) < diff, 1, 0))

        return p_val


    def _multiprocess_ci_loop(self, *args):
        '''
        Multiprocess bootstrapping loop for confidence intervals

        :param c: control data
        :param t: test data
        :return: Mean difference value of single bootstrap iteration
        '''

        boot_c = args[0][0].sample(args[0][0].shape[0], replace = True)
        boot_t = args[0][1].sample(args[0][1].shape[0], replace = True)
        boot_diff = boot_t.mean() - boot_c.mean()

        return boot_diff


    def mean_diff_continuous_ci(self, n = 1000, ci = .95):
        '''
        Bootstrapped mean difference confidence interval on continuous variable
        ----------
        Params:
            control: continuous data array for control group
            test = continuous data array for test group
            n = number of bootstrap iterations (higher is more accurate, more computationally expensive)
        '''
        control, test = self.data_prep

        c = pd.DataFrame({'data': control})
        t = pd.DataFrame({'data': test})

        p = multiprocessing.Pool()
        sample_means = p.map(self._multiprocess_ci_loop, [(c, t) for i in range(n)])

        alpha = ((1 - ci) * 100) / 2

        lb = np.percentile(sample_means, alpha)
        ub = np.percentile(sample_means, 100 - alpha)

        return lb, ub


    def quant_reg(self, quantiles, viz = False):
        '''Perform quantile treatment effect analysis on test & control sets
        -----------------------------------------------------------------
        Params:
            quantiles = list of quantiles
            viz = Boolean value with True outputting histogram

        Return:
            DataFrame with quantiles and associated p-values
        '''

        control, test = self.data_prep

        c = pd.DataFrame({'data': control,
                          'label': 'control'})
        t = pd.DataFrame({'data': test,
                          'label': 'test'})
        comb_df = c.append(t)

        X = pd.get_dummies(comb_df['label'], drop_first = True)
        y = comb_df['data']

        pvalues = []

        for q in quantiles:
            model = sm.QuantReg(y, sm.add_constant(X))
            result = model.fit(q = q, kernel = 'gau')
            pvalues.append(result.pvalues[1])

        out_df = pd.DataFrame({'quantile': ['{:.2f}'.format(i) for i in quantiles],
                               'p-values': pvalues})

        if viz:
            fig, ax = plt.subplots(1, 1, figsize = (6, 6))

            sns.kdeplot(control, color = 'r', ax = ax, label = 'Control')
            sns.kdeplot(test, color = 'b', alpha = 0.5, ax = ax, label = 'Test')
            ax.legend()

            plt.show()

        return out_df


class BinaryTestEval:
    def __init__(self, control, test):
        self.control = control
        self.test = test


    def __repr__(self):
        return 'Class for A/B testing on continuous data'


    @property
    def data_prep(self):
        '''Convert data to numpy arrays for consistency'''
        if type(self.control) != np.ndarray:
            self.control = np.array(self.control)
        if type(self.test) != np.ndarray:
            self.test = np.array(self.control)

        return self.control, self.test


    def binary_pval(self):
        '''Run prop test on binary metrics to get p-value
        ----------
        Params:
            control = binary data array for control group
            test = binary data array for test group
        '''

        control, test = self.data_prep

        count = np.array([control.sum(), test.sum()])
        nobs = np.array([control.shape[0], test.shape[0]])

        pval = sm.stats.proportions_ztest(count, nobs)[1]

        return pval


    def binary_ci(self, ci = .9):
        '''Calc confidence interval to get est. population variance
        ----------
        Params:
            control = binary data array for control group
            test = binary data array for test group
            ci = confidence interval desired

        '''
        control, test = self.data_prep

        cp = np.mean(control)
        tp = np.mean(test)

        c = (cp * (1 - cp)) / control.shape[0]
        t = (tp * (1 - tp)) / test.shape[0]

        z_score = stats.norm.ppf(1 - (1 - ci) / 2)

        t_c = z_score * np.sqrt(c + t)

        ub = tp - cp + t_c
        lb = tp - cp - t_c

        return lb, ub
