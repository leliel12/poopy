#!/usr/bin/env python
# -*- coding: utf-8 -*-


from poopy import script


class Script(script.ScriptBase):

    def map(self, k, v, ctx):
        import random

        import numpy as np
        import scipy

        from sklearn import tree

        attrs = ['sepallength', 'sepalwidth', 'petallength', 'petalwidth']
        random.shuffle(attrs)
        attrs.pop()

        data, meta = v

        target = np.array(data['class'])
        train = np.array(data[attrs][:75])
        X = np.asarray(train.tolist(), dtype=np.float32)
        dt = tree.DecisionTreeClassifier(criterion='entropy',
                                  max_features="auto",
                                  min_samples_leaf=10)
        ctx.emit(None, dt)

    def reduce(self, k, v, ctx):
        for vi in v:
            ctx.emit("iris", vi)

    def setup(self, job):
        job.name = "Random Forest"
        job.input_path.append(["poopyFS://iris.arff", self.readers.ARFFReader])



