class Block:
    def __init__(self, rdd_id, index, user_id, size):
        self.rdd_id=rdd_id
        self.index=index
        self.id = '%s_%s' % (rdd_id, index)
        self.size=size
        self.ref_count = 0
        self.ref_count_by_job = 0
        self.history_frequency = 0

        self.is_shuffle = False
        self.is_complete = True # complete means its input blocks have not been evicted. Initialized as True
        self.is_rdd_complete = True
        self.rdd = 1
        self.eff_ref_count_rdd = 0
        self.eff_ref_count_blk = 0
        self.user_id = user_id
        self.has_been_generated = 0  # flag indicating whether it has been generated: to
