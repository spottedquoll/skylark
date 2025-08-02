function result = no_duplicates(array)
%no_duplicates 
    
    assert(isnumeric(array));
    assert(all_finite(array));
    
    if size(array,1) < size(array,2)
        array = array';
    end
    
    if size(unique(array),1) == size(array,1)
        pass = 1;
    else
        pass = 0;
    end
    
    % return logical
    result = pass == 1;
    
end

