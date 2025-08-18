function write_cell_to_disk(data,fname)
%write_cell_to_disk
% In some cases it appears writecell appends and does not overwrite. This method protects against this behaviour.
% If the destination directory does not exist it will be created

    [filepath,~,~] = fileparts(fname);
    if ~isfolder(filepath); mkdir(filepath); end

    if isfile(fname); delete(fname); end
    writecell(data,fname);

end