
let read_rsa_private_key () =
  try
    let keyfile = Sys.getenv "PROD_KEYFILE" in
    let pem_data = Stdio.In_channel.read_all keyfile in

    match X509.Private_key.decode_pem pem_data with
    | Ok (`RSA key) -> Some key
    | Ok _ ->
        prerr_endline "Key is not an RSA private key";
        None
    | Error (`Msg msg) ->
        prerr_endline ("Error decoding private key: " ^ msg);
        None
  with
  | Not_found ->
      prerr_endline "PROD_KEYFILE environment variable not set";
      None
  | Sys_error msg ->
      prerr_endline ("File error: " ^ msg);
      None
  | ex ->
      prerr_endline ("Error reading private key: " ^ Printexc.to_string ex);
      None

let () =
  match read_rsa_private_key () with
  | Some rsa_key ->
      print_endline "Successfully loaded RSA private key";
      let bits = Mirage_crypto_pk.Rsa.priv_bits rsa_key in 
      Printf.printf "Key size: %d bits\n" bits
  | None ->
      print_endline "Failed to load RSA private key";
      exit 1