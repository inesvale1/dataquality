from __future__ import annotations

import re


def clean_alphanumeric_document(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(value or "")).upper()


def clean_numeric_document(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def calculate_cpf_dv(cpf_base: str) -> str:
    cpf_clean = clean_numeric_document(cpf_base)
    if len(cpf_clean) != 9:
        raise ValueError(f"CPF base must contain exactly 9 digits after cleaning. Received '{cpf_clean}'.")

    def calculate_single_dv(partial: str, start_weight: int) -> str:
        total = sum(int(digit) * weight for digit, weight in zip(partial, range(start_weight, 1, -1)))
        remainder = total % 11
        dv = 0 if remainder < 2 else 11 - remainder
        return str(dv)

    first_dv = calculate_single_dv(cpf_clean, 10)
    second_dv = calculate_single_dv(cpf_clean + first_dv, 11)
    return first_dv + second_dv


def is_valid_cpf(cpf: str) -> bool:
    cpf_clean = clean_numeric_document(cpf)
    if len(cpf_clean) != 11:
        return False
    if len(set(cpf_clean)) == 1:
        return False

    try:
        expected_dv = calculate_cpf_dv(cpf_clean[:9])
    except ValueError:
        return False
    return cpf_clean[9:] == expected_dv


def calculate_cnpj_numeric_dv(cnpj_base: str) -> str:
    cnpj_clean = clean_numeric_document(cnpj_base)
    if len(cnpj_clean) != 12:
        raise ValueError(f"CNPJ base must contain exactly 12 digits after cleaning. Received '{cnpj_clean}'.")

    def calculate_single_dv(partial: str, weights: list[int]) -> str:
        total = sum(int(digit) * weight for digit, weight in zip(partial, weights))
        remainder = total % 11
        dv = 0 if remainder < 2 else 11 - remainder
        return str(dv)

    first_dv_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_dv_weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    first_dv = calculate_single_dv(cnpj_clean, first_dv_weights)
    second_dv = calculate_single_dv(cnpj_clean + first_dv, second_dv_weights)
    return first_dv + second_dv


def is_valid_cnpj_numeric(cnpj: str) -> bool:
    cnpj_clean = clean_numeric_document(cnpj)
    if len(cnpj_clean) != 14:
        return False
    if len(set(cnpj_clean)) == 1:
        return False

    try:
        expected_dv = calculate_cnpj_numeric_dv(cnpj_clean[:12])
    except ValueError:
        return False
    return cnpj_clean[12:] == expected_dv


def calculate_cnpj_alphanumeric_dv(cnpj_base: str) -> str:
    """
    Calculates the two check digits for an alphanumeric CNPJ base.

    The input must contain exactly 12 alphanumeric characters after cleaning.
    """

    def convert_char(char: str) -> int:
        if char.isdigit():
            return int(char)
        if "A" <= char <= "Z":
            return ord(char) - 48
        raise ValueError(f"Invalid character for alphanumeric CNPJ: {char}")

    def calculate_single_dv(partial: str, weights: list[int]) -> str:
        values = [convert_char(char) for char in partial]
        total = sum(value * weight for value, weight in zip(values, weights))
        remainder = total % 11
        dv = 0 if remainder < 2 else 11 - remainder
        return str(dv)

    cnpj_clean = clean_alphanumeric_document(cnpj_base)
    if len(cnpj_clean) != 12:
        raise ValueError(
            "The alphanumeric CNPJ base must contain exactly 12 characters after cleaning. "
            f"Received '{cnpj_clean}' with {len(cnpj_clean)} characters."
        )

    first_dv_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_dv_weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    first_dv = calculate_single_dv(cnpj_clean, first_dv_weights)
    second_dv = calculate_single_dv(cnpj_clean + first_dv, second_dv_weights)
    return first_dv + second_dv


def is_valid_cnpj_alphanumeric(cnpj: str) -> bool:
    """
    Validates a full alphanumeric CNPJ with 14 characters after cleaning.
    """
    cnpj_clean = clean_alphanumeric_document(cnpj)
    if len(cnpj_clean) != 14:
        return False

    base = cnpj_clean[:12]
    provided_dv = cnpj_clean[12:]

    try:
        expected_dv = calculate_cnpj_alphanumeric_dv(base)
    except ValueError:
        return False

    return provided_dv == expected_dv


def is_valid_cnpj(cnpj: str) -> bool:
    cnpj_clean = clean_alphanumeric_document(cnpj)
    if len(cnpj_clean) != 14:
        return False
    if cnpj_clean.isdigit():
        return is_valid_cnpj_numeric(cnpj_clean)
    return is_valid_cnpj_alphanumeric(cnpj_clean)
