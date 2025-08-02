function logical_result = all_positive(array)
%all_finite
    
    result = isempty(find(array < 0));
    
    % return logical
    logical_result = result == 1;

end

