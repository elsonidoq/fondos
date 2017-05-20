from memoized_property import memoized_property
import pandas as pd
from pandas import rolling_corr, rolling_max
from pylab import find
import numpy as np
from fastcluster import linkage
from scipy.cluster.hierarchy import fcluster, leaders, maxdists
from collections import defaultdict
from scipy.cluster.hierarchy import inconsistent, cophenet

class ClusterDataset(object):
    def __init__(self, data, id2obj):
        """
        assumes norm(data[i]) == 1
        """
        self.data = data
        self.id2obj = id2obj
        self.obj2id = {v: k for k, v in id2obj.iteritems()}
        
    def _encode_dists(self, dists):
        N = dists.shape[0]
        encoded_dists = np.zeros(N*(N-1)/2, dtype = dists.dtype)
        pos = 0
        for i, row in enumerate(dists):
            for j in xrange(i+1, len(row)):
                encoded_dists[pos] = row[j]
                pos+=1
        return encoded_dists

    @memoized_property
    def sims(self):
        return self.data.corr().values

    @memoized_property
    def dists(self):
        return 1 - self.sims

    @memoized_property
    def edists(self):
        return self._encode_dists(self.dists)

    @memoized_property
    def Z(self):
        return linkage(self.edists, method='complete')

    def cluster_inconsistency(self, threshold=0.8, percentile=None, depth = 2):
        if percentile is not None: threshold = np.percentile(inconsistent(self.Z)[:,3], percentile)
        cluster_indices = fcluster(self.Z, threshold, depth=depth)
        return cluster_indices

    def cluster_cophenet(self, percentile=1, depth = 2):
        threshold = np.percentile(cophenet(self.Z), percentile)
        print threshold
        cluster_indices = fcluster(self.Z, threshold, criterion='distance')
        return cluster_indices

    def get_clusters_distance(self, cluster_indices):
        L, M = leaders(self.Z, cluster_indices)

        def key(cluster_id):
            L_index = find(M==cluster_id+1)[0]
            node_id = L[L_index]
            if node_id < self.data.shape[0]: return 0
            else: return self.Z[node_id - self.data.shape[0], 2]
            
        return [(c, key(i)) for i, c in enumerate(self.get_human_readable_cluster(cluster_indices))]

    def get_human_readable_cluster(self, cluster_indices):
        clusters = defaultdict(list)
        for obj_id, cluster_idx in enumerate(cluster_indices):
            clusters[cluster_idx].append(self.id2obj[obj_id])
        clusters = sorted(clusters.iteritems(),key=lambda x:x[0])
        return zip(*clusters)[1]
