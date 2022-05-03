from typing import TextIO


class FunctionLoader:
    @staticmethod
    def from_file(fn_file: TextIO) -> str:
        store_contents = False
        contents = []
        for line in fn_file:
            if store_contents:
                contents.append(line)
            elif line.strip() == "### START ###":
                store_contents = True

        raw_text = "".join(contents)
        compile(raw_text, fn_file.name, "exec")
        return raw_text
