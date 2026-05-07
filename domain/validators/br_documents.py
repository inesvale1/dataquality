from __future__ import annotations

import re


def clean_alphanumeric_document(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(value or "")).upper()


def clean_numeric_document(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_numeric_cpf_for_validation(value: str) -> str:
    cpf_clean = clean_numeric_document(value)
    if not cpf_clean or len(cpf_clean) > 11:
        return cpf_clean
    return cpf_clean.zfill(11)


def normalize_numeric_cnpj_for_validation(value: str) -> str:
    cnpj_clean = clean_numeric_document(value)
    if not cnpj_clean or len(cnpj_clean) > 14:
        return cnpj_clean
    return cnpj_clean.zfill(14)


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
    cpf_clean = normalize_numeric_cpf_for_validation(cpf)
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
    cnpj_clean = normalize_numeric_cnpj_for_validation(cnpj)
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
    if cnpj_clean.isdigit():
        return is_valid_cnpj_numeric(cnpj_clean)
    if len(cnpj_clean) != 14:
        return False
    return is_valid_cnpj_alphanumeric(cnpj_clean)


def normalize_numeric_cgf_for_validation(value: str) -> str:
    cgf_clean = clean_numeric_document(value)
    if not cgf_clean or len(cgf_clean) > 9:
        return cgf_clean
    return cgf_clean.zfill(9)


def calculate_cgf_dv(cgf_base: str) -> str:
    """Calculates the check digit for a Ceará CGF (8-digit base)."""
    cgf_clean = clean_numeric_document(cgf_base)
    if len(cgf_clean) != 8:
        raise ValueError(f"CGF base must contain exactly 8 digits. Received '{cgf_clean}'.")
    weights = [9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(int(d) * w for d, w in zip(cgf_clean, weights))
    remainder = total % 11
    dv = 0 if remainder < 2 else 11 - remainder
    return str(dv)


def is_valid_cgf(cgf: str) -> bool:
    """Validates a Ceará CGF (Cadastro Geral da Fazenda): 8 sequential digits + 1 check digit."""
    cgf_clean = normalize_numeric_cgf_for_validation(cgf)
    if len(cgf_clean) != 9:
        return False
    if len(set(cgf_clean)) == 1:
        return False
    try:
        expected_dv = calculate_cgf_dv(cgf_clean[:8])
    except ValueError:
        return False
    return cgf_clean[8] == expected_dv
