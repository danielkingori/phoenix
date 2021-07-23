"""Unit tests for LatentDirichletAllocator."""
import pandas as pd
import scipy.sparse

from phoenix.tag.clustering.latent_dirichlet_allocation import LatentDirichletAllocator
from tests.utils import assert_sparse_matrix_equal


def test_LatentDirichletAllocator_word_matrix():
    input_df = pd.DataFrame(
        [("ID_1", "nice words"), ("ID_2", "test words")], columns=["id", "clean_text"]
    )

    output_lda = LatentDirichletAllocator(input_df)

    assert output_lda.vectorizers["all"]
    # the word matrix will sort by alphabetically, then create a matrix of the presence of that
    # word. In this case the matrix is ["nice", "test", "words"]
    assert_sparse_matrix_equal(
        output_lda.vectorizers["all"]["word_matrix"],
        scipy.sparse.csr_matrix([[1, 0, 1], [0, 1, 1]]),
    )


def test_LatentDirichletAllocator_word_matrix_groups():
    input_df = pd.DataFrame(
        [
            ("ID_1", "nice words", "test"),
            ("ID_2", "test words", "test"),
            ("ID_3", "test words", "test2"),
        ],
        columns=["id", "clean_text", "topic"],
    )

    output_lda = LatentDirichletAllocator(input_df, grouping_column="topic")

    assert output_lda.vectorizers["test"]
    assert output_lda.vectorizers["test2"]
    # the word matrix will sort by alphabetically, then create a matrix of the presence of that
    # word. In this case the matrix is ["nice", "test", "words"]
    assert_sparse_matrix_equal(
        output_lda.vectorizers["test"]["word_matrix"],
        scipy.sparse.csr_matrix([[1, 0, 1], [0, 1, 1]]),
    )
    assert_sparse_matrix_equal(
        output_lda.vectorizers["test2"]["word_matrix"], scipy.sparse.csr_matrix([[1, 1]])
    )
