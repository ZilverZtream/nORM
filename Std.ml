(*
   Standard string utilities.
   String concatenation delegates to the runtime-provided __str_concat,
   which is responsible for allocating an appropriately sized buffer.
*)

external __str_concat : string -> string -> string = "__str_concat"

let ( + ) (lhs : string) (rhs : string) : string = __str_concat lhs rhs
