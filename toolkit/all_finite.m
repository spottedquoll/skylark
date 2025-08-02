function logical_result = all_finite(array)
%all_finite
    
    result = isempty(find(~isfinite(array), 1));
    
    % return logical
    logical_result = result == 1;

end

