function out = thousands_separated(in)

% THOUSANDSEP adds thousands Separators to a 1x1 array.

    import java.text.*
    
    assert(isnumeric(in) || islogical(in));
    
    if in > 1
        in = round(in);
    end

    v = DecimalFormat;

    out = char(v.format(in));

end

