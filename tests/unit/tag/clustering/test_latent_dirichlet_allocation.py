"""Unit tests for LatentDirichletAllocator."""
import mock
import numpy as np
import pandas as pd
import scipy.sparse
from mock import MagicMock

from phoenix.tag.clustering import latent_dirichlet_allocation
from tests.utils import assert_sparse_matrix_equal


def test_LatentDirichletAllocator_word_matrix():
    input_df = pd.DataFrame(
        [("ID_1", "nice words"), ("ID_2", "test words")], columns=["id", "clean_text"]
    )

    output_lda = latent_dirichlet_allocation.LatentDirichletAllocator(input_df)

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

    output_lda = latent_dirichlet_allocation.LatentDirichletAllocator(
        input_df, grouping_column="topic"
    )

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


@mock.patch("phoenix.tag.clustering.latent_dirichlet_allocation.GridSearchCV")
def test_LatentDirichletAllocator_train(mock_search):

    input_df = pd.DataFrame(
        [("ID_1", "nice words"), ("ID_2", "test words")], columns=["id", "clean_text"]
    )
    output_lda = latent_dirichlet_allocation.LatentDirichletAllocator(input_df)

    output_lda.train()
    expected_search_params = {"n_components": [10, 20, 30, 40], "max_iter": [10, 20, 40]}

    mock_search.assert_called_with(mock.ANY, cv=None, param_grid=expected_search_params)
    mock_search.return_value.fit.assert_called_with(output_lda.vectorizers["all"]["word_matrix"])
    assert output_lda.vectorizers["all"]["grid_search_model"] == mock_search.return_value


def test_tag_dataframe():
    input_df = pd.DataFrame(["nice words", "test words"], columns=["clean_text"])
    output_lda = latent_dirichlet_allocation.LatentDirichletAllocator(input_df)

    model = MagicMock()
    model.best_estimator_.transform.return_value = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.3]])
    # override grid search model with a mock
    output_lda.vectorizers["all"]["grid_search_model"] = model

    expected_df = pd.DataFrame(
        [("nice words", "all", 3, 0.3), ("test words", "all", 2, 0.5)],
        columns=["clean_text", "lda_name", "lda_cloud", "lda_cloud_confidence"],
    )
    output_lda.tag_dataframe()

    pd.testing.assert_frame_equal(output_lda.dfs["all"], expected_df)
    model.best_estimator_.transform.assert_called_with(
        output_lda.vectorizers["all"]["word_matrix"]
    )
