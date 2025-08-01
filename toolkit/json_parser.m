function val = json_parser(fname)
%json_parser Parses a json file and saves into a struct variable
    
    fid = fopen(fname); 
    raw = fread(fid,inf); 
    str = char(raw'); 
    fclose(fid); 
    val = jsondecode(str);

end