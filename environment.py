# -*- coding: utf-8 -*-
__author__ = 'nobita'

MODEL = 'tokenizer/model/clf.pkl'
VOCAB = 'tokenizer/model/vocab.pkl'
MAX_LENGTH = 'tokenizer/model/max_length.pkl'

WINDOW_LENGTH = 13
# MAX_SYLLABLE = WINDOW_LENGTH / 6
MAX_SYLLABLE = 2
NUM_DIMENSIONS = WINDOW_LENGTH * 2