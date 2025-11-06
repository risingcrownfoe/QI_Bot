def strip_comments_and_trailing_commas(text: str) -> str:
    """
    Make JSON-with-comments/trailing-commas into strict JSON.
    Supports:
      - // line comments
      - /* block comments */
      - # line comments at line-start
      - trailing commas before } or ]
    Preserves content inside strings.
    """
    out_chars = []
    i = 0
    n = len(text)
    in_string = False
    string_quote = ""
    escape = False
    in_line_comment = False
    in_block_comment = False

    def peek(k: int) -> str:
        return text[i + k] if i + k < n else ""

    # remove comments
    while i < n:
        ch = text[i]
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out_chars.append(ch)
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and peek(1) == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if in_string:
            out_chars.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == string_quote:
                in_string = False
                string_quote = ""
            i += 1
            continue
        if ch in ("'", '"'):
            in_string = True
            string_quote = ch
            out_chars.append(ch)
            i += 1
            continue
        if ch == "/" and peek(1) == "/":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and peek(1) == "*":
            in_block_comment = True
            i += 2
            continue
        if ch == "#":
            # treat as line comment if at start-of-line (ignoring spaces)
            j = len(out_chars) - 1
            while j >= 0 and out_chars[j] in (" ", "\t"):
                j -= 1
            if j < 0 or out_chars[j] == "\n":
                in_line_comment = True
                i += 1
                continue
        out_chars.append(ch)
        i += 1

    # remove trailing commas
    text2 = "".join(out_chars)
    out_chars = []
    i = 0
    n = len(text2)
    in_string = False
    string_quote = ""
    escape = False
    while i < n:
        ch = text2[i]
        if in_string:
            out_chars.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == string_quote:
                in_string = False
                string_quote = ""
            i += 1
            continue
        if ch in ('"', "'"):
            in_string = True
            string_quote = ch
            out_chars.append(ch)
            i += 1
            continue
        if ch == ",":
            k = i + 1
            while k < n and text2[k] in (" ", "\t", "\r", "\n"):
                k += 1
            if k < n and text2[k] in ("}", "]"):
                i += 1
                continue
        out_chars.append(ch)
        i += 1
    return "".join(out_chars)
