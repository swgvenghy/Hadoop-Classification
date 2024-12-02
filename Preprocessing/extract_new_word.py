import numpy as np
import pandas as pd
from soynlp.word import WordExtractor




docs = [[sent.split() for sent in doc.split('  ')] for doc in f]


word_extractor = WordExtractor(
    min_frequency=100,
    min_cohesion_forward=0.05, 
    min_right_branching_entropy=0.0
)

word_extractor.train(sentences)
words = word_extractor.extract()