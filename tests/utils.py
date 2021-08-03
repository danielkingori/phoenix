"""Utility functions for tests."""

from scipy.sparse import csr_matrix


def assert_sparse_matrix_equal(m1: csr_matrix, m2: csr_matrix):
    """Asserts that two sparse matrices are equal.

    Asserts the matrices have the same shape, same dtypes, and that the number of stored items
    is the same
    Raises:
        AssertionError: Sparse matrices are not equal

    Reference: https://stackoverflow.com/questions/30685024/check-if-two-scipy-sparse-csr-matrix
    -are-equal
    """

    assert m1.shape == m2.shape
    assert m1.dtype == m2.dtype
    assert (m1 != m2).nnz == 0
