function meta = initialise_environment(options)
%initialise_environment

    %% Definitions

    % Global root definitions
    meta.n_sec_root_gl = 6357;
    meta.n_reg_root_gl = 221;

    %% Toolboxes

    % External tools
    addpath(genpath([options.env.isa_tools 'root_country_legend_toolbox/']));
    addpath(genpath([options.env.isa_tools 'routines_library/']));

    % Root country legend
    meta.root_country_legend = read_root_country_legend([options.env.ielab_global_root '/Legends/']);

    % Tensor toolbox
    assert(isfolder(options.env.tensor_toolbox_path));
    addpath(genpath(options.env.tensor_toolbox_path));

    %% Paths

    save_dir = options.env.save_dir;
    if ~isfolder(save_dir)
        mkdir(save_dir);
    end
    meta.save_dir = save_dir;

    conc_dir = [options.env.comtrade_dir 'concordances/'];
    assert(isfolder(conc_dir));
    
    meta.conc_dir = conc_dir;


end