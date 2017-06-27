class RDD:
    def __init__(self, id, partitions):
        self.id = id
        self.blocks = list()
        self.partitions = partitions
        self.ref_count = 0

        self.is_shuffle = False
        self.is_complete = True
        self.user_id = 1
        self.to_ref_rdds = list()
        self.ref_me_rdds = list()
    def ref_me_block_incomplete(self, index):
        if self.is_shuffle:
            for block in self.blocks:
                block.eff_ref_count_blk -= 1
        else:
            self.blocks[index].eff_ref_count_blk -= 1

    def ref_me_rdd_incomplete(self, partitions):
        for block in self.blocks:
            if self.is_shuffle:
                block.eff_ref_count_rdd -= partitions
            else:
                block.eff_ref_count_rdd -= 1
