
.PHONY: build clean

build:
	ocamlfind ocamlopt -annot -g -linkpkg -package extlib,deriving.syntax,bitstring.syntax -syntax camlp4o sphinx.ml test.ml -o test.native

clean:
	rm -f *.cm* *.annot *.native *.o*

